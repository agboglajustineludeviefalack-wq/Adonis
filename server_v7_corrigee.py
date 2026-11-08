import os
import re
import json
import time
import html
import base64
import asyncio
import hashlib
import secrets
from bisect import bisect_right
from collections import deque
from urllib.parse import urlencode

import aiohttp
from aiohttp import web

# ------------------------------------------------------------------
# CONFIGURATION (variables à mettre dans Railway > Variables)
#   DERIV_APP_ID : l'ID de ton application sur developers.deriv.com
#   NTFY_TOPIC   : le nom secret de ton canal d'alertes ntfy
#   PUBLIC_URL   : (optionnel) l'adresse publique de ton site Railway
#   SCAN_SYMBOLS : (optionnel) marchés à scanner, séparés par des virgules
# ------------------------------------------------------------------
APP_ID = os.environ.get("DERIV_APP_ID", "").strip()
PUBLIC_URL = os.environ.get(
    "PUBLIC_URL", "https://adonis-production-8b23.up.railway.app"
).rstrip("/")
REDIRECT_URI = PUBLIC_URL + "/callback"

AUTH_URL = "https://auth.deriv.com/oauth2/auth"
TOKEN_URL = "https://auth.deriv.com/oauth2/token"
API_BASE = "https://api.derivws.com"
PUBLIC_WS = os.environ.get(
    "DERIV_PUBLIC_WS", "wss://api.derivws.com/trading/v1/options/ws/public"
)

TIMEOUT = aiohttp.ClientTimeout(total=20)
ACCOUNT_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

# --- Scanner ---
_raw_topic = os.environ.get("NTFY_TOPIC", "").strip()
NTFY_TOPIC = _raw_topic if re.match(r"^[A-Za-z0-9_-]{8,64}$", _raw_topic) else ""
_DEFAULT_SYMBOLS = (
    "frxXAUUSD,cryBTCUSD,frxEURUSD,frxUSDJPY,frxAUDCAD,"
    "1HZ50V,BOOM500,CRASH500,CRASH1000,BOOM1000,JD10,stpRNG,frxBROUSD|oil"
)
_cfg = [x.strip() for x in os.environ.get("SCAN_SYMBOLS", _DEFAULT_SYMBOLS).split(",") if x.strip()]
SYMBOLS = [x.split("|")[0].strip() for x in _cfg]
HINTS = {x.split("|")[0].strip(): x.split("|")[1].strip() for x in _cfg if "|" in x}
# Noms tels qu'ils apparaissent dans ton MT5 (pour les alertes et la page)
MT5 = {
    "frxXAUUSD": "XAUUSD", "cryBTCUSD": "BTCUSD", "frxEURUSD": "EURUSD", "frxUSDJPY": "USDJPY",
    "frxAUDCAD": "AUDCAD", "1HZ50V": "Volatility 50 (1s)", "BOOM500": "Boom 500", "CRASH500": "Crash 500",
    "CRASH1000": "Crash 1000", "BOOM1000": "Boom 1000", "JD10": "Jump 10", "stpRNG": "Step Index",
    "frxBROUSD": "US Oil",
}
# Boom : spikes vers le haut -> achats seulement. Crash : spikes vers le bas -> ventes seulement.
SIDE_ONLY = {"BOOM500": "buy", "BOOM1000": "buy", "CRASH500": "sell", "CRASH1000": "sell"}
RESOLVED = {}   # nom demandé -> vrai code chez Deriv (si on a dû le chercher par mot-clé)
NAMES = {}      # code Deriv -> nom affiché par Deriv
PD_RANGE = "H4"  # V7 : Fibonacci exclusivement H4
SWING_N = int(os.environ.get("SWING_N", "2"))        # bougies de chaque côté d'un sommet/creux
MIN_RR = 0.0  # V7 : aucun filtre minimum de RR
EQ_TOL = float(os.environ.get("EQ_TOL", "0.0005"))
OTE_LOW = float(os.environ.get("OTE_LOW", "0.62"))
OTE_HIGH = float(os.environ.get("OTE_HIGH", "0.79"))
SL_ATR_MULT = float(os.environ.get("SL_ATR_MULT", "0.10"))
DAILY_FVG_TOL_ATR = float(os.environ.get("DAILY_FVG_TOL_ATR", "0.25"))   # tolérance des Equal Highs/Lows (0.05 %)
SCAN_EVERY = int(os.environ.get("SCAN_EVERY", "60"))  # secondes entre deux analyses
H4_TTL = 240                                          # secondes avant de retélécharger le H4
BT_DAYS = int(os.environ.get("BT_DAYS", "180"))        # durée du test historique (jours)

# Mémoire du serveur (se vide si Railway redémarre : il faudra se reconnecter)
SESSIONS = {}  # sid -> {"token": ..., "exp": ...}
PENDING = {}   # state -> {"verifier": ..., "ts": ...}

STATE = {"last": None, "error": None, "symbols": {}, "available": []}
SIGNALS = deque(maxlen=30)
SENT = set()
BT = {"running": False, "done": False, "progress": "", "error": None, "results": None}


HTML_CONTENT = r"""
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Adonis V7 - Terminal Pro</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f1117; color: #f0f0f0; padding: 15px; margin: 0; }
  .card { background: #1a1c23; border: 1px solid #2a2e39; border-radius: 8px; padding: 15px; margin-bottom: 15px; }
  h3 { margin-top: 0; color: #38bdf8; font-size: 18px; }
  select { width: 100%; padding: 10px; margin-bottom: 10px; border-radius: 5px; background: #222; color: white; border: 1px solid #444; box-sizing: border-box; }
  .row { display: flex; gap: 10px; margin-bottom: 10px; }
  .col { flex: 1; }
  .btn { display: block; box-sizing: border-box; text-align: center; text-decoration: none; border: none; padding: 10px; border-radius: 5px; width: 100%; font-weight: bold; cursor: pointer; font-size: 14px; color: white; }
  .btn-blue { background: #2563eb; }
  .btn-red { background: #ef4444; }
  .btn-gray { background: #374151; }
  .live-price { font-family: monospace; color: #4ade80; font-weight: bold; font-size: 22px; }
  .log-item { font-size: 12px; border-left: 3px solid #38bdf8; margin-bottom: 6px; color: #cbd5e1; font-family: monospace; background: #111827; padding: 6px; border-radius: 4px; word-break: break-word; }
  .small { font-size: 12px; color: #94a3b8; }
  pre { white-space: pre-wrap; word-break: break-word; font-size: 11px; background: #111827; padding: 8px; border-radius: 4px; max-height: 250px; overflow: auto; }
</style>
</head>
<body>
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:15px;">
    <h1 style="margin:0; font-size:22px; color:#38bdf8;">⚡ Adonis Pro</h1>
    <span id="connectionBadge" style="background:#eab308; color:white; padding:6px 12px; border-radius:20px; font-size:12px;">Déconnecté</span>
  </div>

  <div class="card" id="loginCard" style="display:none;">
    <h3>🔐 Connexion à Deriv</h3>
    <p class="small">Tu vas être redirigé vers Deriv pour autoriser l'application (droit demandé : trade). Ton token reste sur le serveur, jamais dans ton navigateur.</p>
    <a class="btn btn-blue" href="/login">Se connecter avec Deriv</a>
  </div>

  <div class="card" id="scanCard" style="display:none;">
    <h3>🛰️ Scanner (tourne sur le serveur)</h3>
    <div id="scanMeta" class="small" style="margin-bottom:8px;"></div>
    <div id="scanList"></div>
    <details style="margin-top:10px;">
      <summary class="small">Marchés disponibles chez Deriv</summary>
      <pre id="scanSymbols"></pre>
    </details>
  </div>

  <div class="card" id="btCard" style="display:none;">
    <h3>🧪 Test historique</h3>
    <p class="small">Rejoue tes règles sur le passé, marché par marché. Résultats en « R » : 1 R = ce que tu risques sur un trade (perdu = -1 R, gagné = +gain/risque). Les coûts réels (spread, glissement, commission) ne sont pas inclus.</p>
    <button class="btn btn-blue" id="btBtn" onclick="startBacktest()">Lancer le test</button>
    <div id="btStatus" class="small" style="margin-top:8px;"></div>
    <div id="btList" style="margin-top:8px;"></div>
  </div>

  <div class="card" id="accountCard" style="display:none;">
    <h3>🔗 Compte & Connexion</h3>
    <label class="small">COMPTE</label>
    <select id="accountSelect"></select>
    <div class="row">
      <button class="btn btn-blue" onclick="connectWs()">Se connecter</button>
      <button class="btn btn-red" onclick="closeWs()">Couper</button>
    </div>
    <div class="row">
      <div class="col">
        <label class="small">COMPTE ACTIF</label>
        <div id="brokerDisplay" style="font-family:monospace; color:#f59e0b; font-weight:bold; margin-top:6px;">Non connecté</div>
      </div>
      <div class="col">
        <label class="small">SOLDE</label>
        <div id="accountBalance" style="font-family:monospace; font-size:18px; color:#4ade80; font-weight:bold; margin-top:4px;">--</div>
      </div>
    </div>
    <a class="btn btn-gray" href="/logout">Se déconnecter de Deriv</a>
    <details style="margin-top:10px;">
      <summary class="small">Réponse brute de Deriv (debug)</summary>
      <pre id="debug"></pre>
    </details>
  </div>

  <div class="card" id="radarCard" style="display:none;">
    <h3>📡 Marché en direct</h3>
    <select id="marketSelect" onchange="subscribeTicker()"></select>
    <div>Prix : <span id="livePrice" class="live-price">--</span></div>
    <p class="small">Cette zone affiche seulement le prix. Le classement A+/A/B est fait par le scanner du serveur (carte ci-dessus).</p>
  </div>

  <div class="card">
    <h3>📋 Journal de connexion</h3>
    <div id="log" style="max-height:240px; overflow-y:auto;"></div>
  </div>

<script>
const $ = id => document.getElementById(id);
let ws = null;
let reqId = 1;

const defaultMarkets = [
  ["1HZ100V", "Volatility 100 (1s)"],
  ["frxXAUUSD", "XAUUSD (Or)"],
  ["R_50", "Volatility 50"],
  ["stpRNG", "Step Index"],
  ["BOOM500", "BOOM 500"],
  ["CRASH500", "Crash 500"]
];

function log(msg) {
  const d = document.createElement("div");
  d.className = "log-item";
  d.textContent = new Date().toLocaleTimeString() + "  " + msg;
  $("log").prepend(d);
}

function setBadge(text, color) {
  const b = $("connectionBadge");
  b.textContent = text;
  b.style.background = color;
}

function fillMarkets(list, keep) {
  const sel = $("marketSelect");
  sel.innerHTML = "";
  list.forEach(m => {
    const o = document.createElement("option");
    o.value = m[0];
    o.textContent = m[1];
    sel.appendChild(o);
  });
  if (keep) {
    for (const o of sel.options) { if (o.value === keep) { sel.value = keep; } }
  }
}

function send(obj) {
  if (!ws || ws.readyState !== 1) return;
  obj.req_id = reqId++;
  ws.send(JSON.stringify(obj));
}

function subscribeTicker() {
  if (!ws || ws.readyState !== 1) return;
  send({ forget_all: "ticks" });
  $("livePrice").textContent = "--";
  send({ ticks: $("marketSelect").value, subscribe: 1 });
}

async function loadScanner() {
  let r;
  try { r = await fetch("/api/scanner"); } catch (e) { return; }
  if (r.status === 401) return;
  const j = await r.json();
  $("scanCard").style.display = "block";
  const when = j.derniere_analyse ? new Date(j.derniere_analyse * 1000).toLocaleTimeString() : "pas encore";
  $("scanMeta").textContent = "Dernière analyse : " + when + " · alertes ntfy : " + (j.ntfy_etat || (j.ntfy ? "actives" : "non configurées")) + (j.erreur ? " · erreur : " + j.erreur : "");
  const list = $("scanList");
  list.innerHTML = "";
  (j.signaux || []).forEach(s => {
    const d = document.createElement("div");
    d.className = "log-item";
    d.style.whiteSpace = "pre-line";
    d.style.borderLeftColor = "#22c55e";
    d.textContent = "🚨 " + s.heure + "\n" + s.texte;
    list.appendChild(d);
  });
  Object.keys(j.symboles || {}).forEach(sym => {
    const d = document.createElement("div");
    d.className = "log-item";
    const si = j.symboles[sym];
    d.textContent = (si.label || sym) + " — " + si.resume;
    list.appendChild(d);
  });
  $("scanSymbols").textContent = (j.disponibles || []).map(x => x[0] + " = " + x[1]).join("\n");
}

let btTimer = null;

async function startBacktest() {
  $("btBtn").disabled = true;
  $("btStatus").textContent = "Démarrage...";
  try { await fetch("/api/backtest/start", { method: "POST" }); } catch (e) { $("btStatus").textContent = "Erreur : " + e; return; }
  if (btTimer) clearInterval(btTimer);
  btTimer = setInterval(loadBacktest, 3000);
  loadBacktest();
}

const MODE_NAMES = {
  fvg: "Retest FVG confirmé — entrée à la clôture M15",
  close: "Entrée à la clôture du retour dans le range"
};
const sign = x => (Number(x) >= 0 ? "+" : "") + x;

async function loadBacktest() {
  let r;
  try { r = await fetch("/api/backtest"); } catch (e) { return; }
  if (r.status === 401) return;
  const j = await r.json();
  $("btStatus").textContent = j.error ? "Erreur : " + j.error : (j.running ? "En cours : " + j.progress : (j.done ? "Terminé." : ""));
  if (!j.running) {
    $("btBtn").disabled = false;
    if (btTimer) { clearInterval(btTimer); btTimer = null; }
  }
  const list = $("btList");
  list.innerHTML = "";
  if (!j.results) return;
  const res = j.results;
  const pct = (a, b) => b ? Math.round(a * 100 / b) : 0;
  Object.keys(res.entonnoir || {}).forEach(mode => {
    const f = res.entonnoir[mode];
    const c2 = res.combos.find(c => c.mode === mode && c.min_rr === 2);
    const d = document.createElement("div");
    d.className = "log-item";
    d.textContent = MODE_NAMES[mode] + " — entonnoir (" + res.marches + " marchés, " + res.jours + " jours) : biais H4 exploitable " + pct(f.bias, f.steps) + " % du temps · bon côté de l'équilibre " + pct(f.zone, f.steps) + " % · CRT avec sweep : " + f.crt + " · entrées possibles : " + f.fvg + " · gain/risque < 2 : " + f.rr + " · signaux (RR ≥ 2) : " + (c2 ? c2.total.setups : 0);
    list.appendChild(d);
  });
  res.combos.forEach(c => {
    const t = c.total;
    const done = t.gagnes + t.perdus + (t.equilibres || 0) + t.expires;
    const per = done ? (t.r_total / done).toFixed(2) : "0.00";
    const det = document.createElement("details");
    det.className = "log-item";
    det.style.borderLeftColor = t.r_total > 0 ? "#22c55e" : "#ef4444";
    const sm = document.createElement("summary");
    sm.textContent = MODE_NAMES[c.mode] + " · RR ≥ " + c.min_rr + " : " + t.setups + " trades · " + t.gagnes + " gagnés · " + t.perdus + " perdus · " + (t.equilibres || 0) + " à l'équilibre · " + sign(t.r_total) + " R (" + sign(per) + " R/trade) · avant les " + res.jours_new + " derniers jours : " + t.n_old + " trades, " + sign(t.r_old) + " R · " + res.jours_new + " derniers jours : " + t.n_new + " trades, " + sign(t.r_new) + " R";
    det.appendChild(sm);
    const audit = document.createElement("div");
    audit.style.marginTop = "6px";
    audit.style.padding = "8px";
    audit.style.background = "rgba(255,255,255,.04)";
    const p = t.periods || [];
    const pf = t.profit_factor === null ? "—" : (t.profit_factor === "inf" ? "∞" : t.profit_factor);
    audit.textContent = "Audit : PF " + pf + " · espérance " + sign(t.expectancy_r) + " R/trade · drawdown max " + t.max_drawdown_r + " R · série max pertes " + t.max_pertes_consecutives +
      " · sans top 1/2/3/5 : " + [t.sans_best1_r, t.sans_best2_r, t.sans_best3_r, t.sans_best5_r].map(x => sign(x) + " R").join(" / ") +
      " · périodes : " + p.map((x, i) => "P" + (i + 1) + " " + x.n + " trades / " + sign(x.r) + " R").join(" · ");
    det.appendChild(audit);
    c.lignes.forEach(l => {
      const d = document.createElement("div");
      d.style.marginTop = "4px";
      const bpf = l.achats_pf === null ? "—" : (l.achats_pf === "inf" ? "∞" : l.achats_pf);
      const spf = l.ventes_pf === null ? "—" : (l.ventes_pf === "inf" ? "∞" : l.ventes_pf);
      d.textContent = l.erreur ? (l.symbole + " — erreur : " + l.erreur) : (l.symbole + " — " + l.setups + " trades · " + l.gagnes + " G · " + l.perdus + " P · " + sign(l.r_total) + " R · PF " + (l.profit_factor === null ? "—" : (l.profit_factor === "inf" ? "∞" : l.profit_factor)) + " · DD " + l.max_drawdown_r + " R · ACHATS " + l.achats + " trades / " + sign(l.r_achats) + " R / PF " + bpf + " / esp. " + sign(l.achats_expectancy_r) + " R · VENTES " + l.ventes + " trades / " + sign(l.r_ventes) + " R / PF " + spf + " / esp. " + sign(l.ventes_expectancy_r) + " R");
      det.appendChild(d);
    });
    list.appendChild(det);
  });
  const note = document.createElement("div");
  note.className = "small";
  note.style.marginTop = "6px";
  note.textContent = "V7 : Fibonacci H4 OTE 62–79 %, CRT H4, FVG M15 rejeté, SL derrière le FVG avec buffer ATR, TP liquidité H4/D1 et trailing par FVG M15. Les seuils RR affichés sont des statistiques, jamais des filtres d’entrée.";
  list.appendChild(note);
}

async function init() {
  fillMarkets(defaultMarkets, null);
  let me;
  try {
    me = await (await fetch("/api/me")).json();
  } catch (e) {
    log("Serveur injoignable : " + e);
    return;
  }
  if (!me.app_id_ok) log("DERIV_APP_ID est vide : ajoute-le dans Railway > Variables.");
  if (!me.logged_in) {
    $("loginCard").style.display = "block";
    return;
  }
  $("accountCard").style.display = "block";
  $("btCard").style.display = "block";
  loadBacktest();
  loadScanner();
  setInterval(loadScanner, 30000);
  await loadAccounts();
}

async function loadAccounts() {
  const sel = $("accountSelect");
  sel.innerHTML = "";
  let res;
  try {
    res = await (await fetch("/api/accounts")).json();
  } catch (e) {
    log("Erreur en chargeant les comptes : " + e);
    return;
  }
  $("debug").textContent = JSON.stringify(res, null, 2);
  if (res.status === 401 || res.error === "not_logged_in") {
    log("Session expirée : reconnecte-toi à Deriv.");
    $("accountCard").style.display = "none";
    $("loginCard").style.display = "block";
    return;
  }
  const body = res.body || {};
  const list = Array.isArray(body) ? body : (body.data || body.accounts || []);
  let n = 0;
  (Array.isArray(list) ? list : []).forEach(acc => {
    const id = acc.account_id || acc.accountId || acc.id || acc.loginid;
    if (!id) return;
    const type = acc.account_type || acc.type || "";
    const bal = (acc.balance !== undefined) ? acc.balance + " " + (acc.currency || "") : "";
    const o = document.createElement("option");
    o.value = id;
    o.textContent = [id, type, bal].filter(Boolean).join(" · ");
    sel.appendChild(o);
    n++;
  });
  if (n === 0) {
    const o = document.createElement("option");
    o.value = "";
    o.textContent = "Aucun compte reconnu (voir debug)";
    sel.appendChild(o);
    log("Réponse reçue mais format non reconnu : ouvre le debug.");
  } else {
    log(n + " compte(s) trouvé(s).");
  }
}

function closeWs() {
  if (ws) {
    ws.onclose = null;
    try { ws.close(); } catch (e) {}
    ws = null;
  }
  setBadge("Déconnecté", "#eab308");
  $("radarCard").style.display = "none";
  $("brokerDisplay").textContent = "Non connecté";
  $("accountBalance").textContent = "--";
}

async function connectWs() {
  const id = $("accountSelect").value;
  if (!id) { log("Choisis d'abord un compte."); return; }
  closeWs();
  setBadge("Connexion...", "#eab308");
  log("Demande d'un OTP pour " + id + "...");

  let j;
  try {
    const r = await fetch("/api/otp", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ account_id: id })
    });
    j = await r.json();
  } catch (e) {
    log("Erreur réseau vers ton serveur : " + e);
    setBadge("Erreur", "#ef4444");
    return;
  }
  if (!j.url) {
    log("OTP refusé : " + JSON.stringify(j));
    setBadge("Erreur OTP", "#ef4444");
    return;
  }
  log("OTP reçu. Ouverture du WebSocket...");

  ws = new WebSocket(j.url);
  const timer = setTimeout(() => {
    if (ws && ws.readyState !== 1) {
      log("Délai dépassé (15 s) : le WebSocket ne s'ouvre pas.");
      setBadge("Délai dépassé", "#ef4444");
    }
  }, 15000);

  ws.onopen = () => {
    clearTimeout(timer);
    setBadge("Connecté (Live)", "#22c55e");
    $("brokerDisplay").textContent = "Deriv — " + id;
    $("radarCard").style.display = "block";
    log("WebSocket ouvert.");
    send({ balance: 1, subscribe: 1 });
    send({ active_symbols: "brief" });
    subscribeTicker();
  };

  ws.onmessage = ev => {
    let d;
    try { d = JSON.parse(ev.data); } catch (e) { return; }
    if (d.error) {
      log("Erreur API (" + (d.msg_type || "?") + ") : " + (d.error.message || JSON.stringify(d.error)));
      return;
    }
    if (d.msg_type === "balance" && d.balance) {
      $("accountBalance").textContent = Number(d.balance.balance).toFixed(2) + " " + (d.balance.currency || "");
    } else if (d.msg_type === "tick" && d.tick) {
      $("livePrice").textContent = String(d.tick.quote);
    } else if (d.msg_type === "active_symbols" && Array.isArray(d.active_symbols)) {
      const items = [];
      d.active_symbols.forEach(s => {
        const sid = s.underlying_symbol || s.symbol;
        if (sid) items.push([sid, s.underlying_symbol_name || s.display_name || sid]);
      });
      if (items.length > 0) {
        items.sort((a, b) => a[1].localeCompare(b[1]));
        fillMarkets(items, $("marketSelect").value);
        log(items.length + " marchés reçus de Deriv.");
        subscribeTicker();
      }
    }
  };

  ws.onerror = () => {
    log("Erreur WebSocket (la connexion a échoué).");
    setBadge("Erreur WebSocket", "#ef4444");
  };

  ws.onclose = ev => {
    clearTimeout(timer);
    log("WebSocket fermé (code " + ev.code + ").");
    setBadge("Déconnecté", "#eab308");
    $("radarCard").style.display = "none";
  };
}

window.onload = init;
</script>
</body>
</html>
"""


# ==================================================================
#  ANALYSE (CRT H4 + biais Daily/H4 + discount/premium + FVG M15)
#  Les fonctions ci-dessous ne font aucun appel réseau.
#  Chaque bougie = {"t": début en secondes, "o", "h", "l", "c"}
#  Pour les VENTES, on inverse les prix (miroir) et on réutilise
#  exactement la logique des ACHATS, puis on remet les prix à l'endroit.
# ==================================================================
RANK = {"no_range": 0, "not_in_zone": 1, "no_crt": 2, "no_sweep": 3,
        "no_reentry": 4, "no_fvg": 5, "waiting_retrace": 6, "rr_low": 7, "signal": 8}
ARROW = {"up": "↑", "down": "↓", None: "?"}
MARKET_HISTORY = {}


def real(sym): return RESOLVED.get(sym, sym)
def label(sym): return MT5.get(sym, sym)
def side_allowed(only, side): return not only or only == side
def pick_side(b_d1, b_h4): return b_h4  # H4 dirige; D1 sert aux liquidités, sans veto
def fmt(x): return ("%.2f" % x) if abs(x) >= 100 else ("%.5f" % x)
def split_closed(cs, gran, now):
    closed=[c for c in cs if c["t"]+gran<=now]
    forming=[c for c in cs if c["t"]+gran>now]
    return closed,(forming[-1] if forming else None)

def swings(cs, n=SWING_N):
    highs=[]; lows=[]
    for i in range(n,len(cs)-n):
        h,l=cs[i]["h"],cs[i]["l"]
        if all(h>cs[i-k]["h"] for k in range(1,n+1)) and all(h>=cs[i+k]["h"] for k in range(1,n+1)): highs.append((i,h))
        if all(l<cs[i-k]["l"] for k in range(1,n+1)) and all(l<=cs[i+k]["l"] for k in range(1,n+1)): lows.append((i,l))
    return highs,lows

def structure_bias(cs,n=SWING_N):
    hs,ls=swings(cs,n)
    if len(hs)<2 or len(ls)<2: return None
    if hs[-1][1]>hs[-2][1] and ls[-1][1]>ls[-2][1]: return "up"
    if hs[-1][1]<hs[-2][1] and ls[-1][1]<ls[-2][1]: return "down"
    return None

def fib_ote(cs, side):
    """Dernier swing confirmé opposé -> swing confirmé le plus récent, sans lookahead."""
    hs,ls=swings(cs,SWING_N)
    if not hs or not ls: return None
    if side=="buy":
        hi_i,hi=hs[-1]
        prior=[(i,v) for i,v in ls if i<hi_i]
        if not prior: return None
        lo_i,lo=prior[-1]
        if hi<=lo: return None
        span=hi-lo
        return (hi-span*OTE_HIGH, hi-span*OTE_LOW, lo, hi)
    lo_i,lo=ls[-1]
    prior=[(i,v) for i,v in hs if i<lo_i]
    if not prior: return None
    hi_i,hi=prior[-1]
    if hi<=lo: return None
    span=hi-lo
    return (lo+span*OTE_LOW, lo+span*OTE_HIGH, lo, hi)

def atr(cs, period=14):
    if len(cs)<period+1: return None
    trs=[]
    for a,b in zip(cs[-period-1:-1],cs[-period:]):
        trs.append(max(b["h"]-b["l"],abs(b["h"]-a["c"]),abs(b["l"]-a["c"])))
    return sum(trs)/len(trs) if trs else None

def fvg_zones(cs, side):
    out=[]
    for i in range(2,len(cs)):
        a,c=cs[i-2],cs[i]
        if side=="buy" and c["l"]>a["h"]: out.append((i,a["h"],c["l"],cs[i]["t"]))
        if side=="sell" and c["h"]<a["l"]: out.append((i,c["h"],a["l"],cs[i]["t"]))
    return out

def nearest_liquidity(entry,h4c,side,crt):
    hs,ls=swings(h4c[-250:],SWING_N)
    levels=[v for _,v in (hs if side=="buy" else ls) if (v>entry if side=="buy" else v<entry)]
    edge=crt["h"] if side=="buy" else crt["l"]
    if (edge>entry if side=="buy" else edge<entry): levels.append(edge)
    if not levels: return None,"aucune liquidité H4 identifiée"
    target=min(levels) if side=="buy" else max(levels)
    return target,"liquidité H4 (swing/CRT)"

def daily_fvg_target(d1c,h4_target,side,m15c):
    zones=fvg_zones(d1c,side)
    av=atr(m15c) or abs(h4_target)*0.0005
    tol=av*DAILY_FVG_TOL_ATR
    candidates=[]
    for _,lo,hi,t in zones:
        # Un pool D1 doit être sur le chemin et chevaucher/approcher le TP H4.
        if lo-tol<=h4_target<=hi+tol:
            candidates.append((lo,hi,t))
    if not candidates: return h4_target,"liquidité H4"
    lo,hi,_=min(candidates,key=lambda z:abs((z[0]+z[1])/2-h4_target))
    return ((hi if side=="buy" else lo)),"FVG D1 au niveau du TP H4"

def classify_setup(sym,sig):
    rr=float(sig.get("rr",0) or 0)
    return ("A+" if rr>=3.5 else "A" if rr>=2 else "B", "classement descriptif uniquement; aucun filtre historique")

def refresh_market_history(alltr): return None

def _find_side_signal(side,h4c,h4f,m15c,d1c):
    if len(h4c)<20 or len(m15c)<30: return {"code":"no_range"}
    fib=fib_ote(h4c,side)
    if not fib: return {"code":"no_range"}
    zlo,zhi,swing_lo,swing_hi=fib
    px=m15c[-1]["c"]
    # CRT : la première H4 définit le range; la seconde balaie un seul côté puis clôture dedans.
    pairs=[]
    # CRT confirmé uniquement à la clôture de la deuxième bougie H4.
    if len(h4c)>=2: pairs.append((h4c[-2],h4c[-1]))
    for c1,c2 in reversed(pairs):
        sweep=(c2["l"]<c1["l"] and c2["h"]<=c1["h"]) if side=="buy" else (c2["h"]>c1["h"] and c2["l"]>=c1["l"])
        reentry=(c1["l"]<c2["c"]<=c1["h"]) if side=="buy" else (c1["l"]<=c2["c"]<c1["h"])
        if not (sweep and reentry): continue
        if not zlo <= c2["c"] <= zhi: continue
        # M15 FVG formé après la clôture de réintégration H4.
        post=[m for m in m15c if m["t"]>=c2["t"]+14400]
        if len(post)<4: continue
        zones=fvg_zones(post,side)
        if not zones: continue
        # Le dernier FVG valide est suivi; rejet = retest du bord proche et clôture hors zone.
        last=post[-1]; chosen=None
        for idx,lo,hi,ft in reversed(zones):
            later=post[idx+1:]
            invalid=any((x["c"]<lo if side=="buy" else x["c"]>hi) for x in later)
            if invalid: continue
            if side=="buy": rejected=last["l"]<=hi and last["h"]>=lo and last["c"]>hi
            else: rejected=last["h"]>=lo and last["l"]<=hi and last["c"]<lo
            if rejected: chosen=(lo,hi,ft); break
        if not chosen: return {"code":"waiting_retrace","c1_t":c1["t"]}
        lo,hi,ft=chosen
        entry=last["c"]
        a=atr(m15c) or abs(entry)*0.0005
        buffer=a*SL_ATR_MULT
        sl=(lo-buffer) if side=="buy" else (hi+buffer)
        risk=abs(entry-sl)
        if risk<=0: continue
        tp,tp_label=nearest_liquidity(entry,h4c,side,c1)
        if tp is None: continue
        tp,tp_label=daily_fvg_target(d1c,tp,side,m15c)
        if (tp<=entry if side=="buy" else tp>=entry): continue
        rr=abs(tp-entry)/risk
        return {"code":"signal","side":side,"entry":entry,"sl":sl,"tp":tp,"tp_label":tp_label,
                "rr":rr,"price":px,"c1_t":c1["t"],"c1_high":c1["h"],"c1_low":c1["l"],
                "fvg_t":ft,"fvg_bottom":lo,"fvg_top":hi,"reentry_t":c2["t"],
                "reentry_o":c2["o"],"reentry_h":c2["h"],"reentry_l":c2["l"],"reentry_c":c2["c"],
                "sweep_t":c2["t"],"sweep_low":c2["l"],"ote_low":zlo,"ote_high":zhi}
    return {"code":"not_in_zone" if not (zlo <= px <= zhi) else "no_crt","ote_low":zlo,"ote_high":zhi}

def analyze_core(d1c,h4c,h4f,m15c,m15f,biases=None,only=None,mode=None,min_rr=None):
    if len(d1c)<20 or len(h4c)<30 or len(m15c)<30:
        return {"resume":"historique insuffisant"},None
    bias=structure_bias(h4c,SWING_N)
    side="buy" if bias=="up" else "sell" if bias=="down" else None
    if side is None: return {"resume":"structure H4 indéterminée; pas de setup"},None
    if not side_allowed(only,side): return {"resume":"restriction directionnelle du marché"},None
    res=_find_side_signal(side,h4c,h4f,m15c,d1c)
    msg={"no_range":"swings confirmés insuffisants pour Fibonacci","not_in_zone":"prix hors OTE 62–79 %",
         "no_crt":"pas de CRT H4 valide dans l'OTE","waiting_retrace":"FVG M15 en attente de rejet","signal":"SETUP V7"}
    info={"resume":"H4 %s · %s"%(ARROW[bias],msg.get(res.get("code"),"analyse V7")),"code":res.get("code"),"c1_t":res.get("c1_t"),"fvg_t":res.get("fvg_t"),"rr":res.get("rr")}
    if res.get("code")!="signal": return info,None
    return info,res

def analyze(sym,d1,h4,m15,now):
    if m15 and now-(m15[-1]["t"]+900)>1800: return {"resume":"marché fermé ou données anciennes"},None
    d1c,_=split_closed(d1,86400,now); h4c,h4f=split_closed(h4,14400,now); m15c,m15f=split_closed(m15,900,now)
    info,sig=analyze_core(d1c,h4c,h4f,m15c,m15f,None,SIDE_ONLY.get(sym))
    if sig:
        grade,reason=classify_setup(sym,sig); sig["grade"]=grade; sig["grade_reason"]=reason
        info["grade"]=grade; info["grade_reason"]=reason; info["resume"]+=" · "+grade
    return info,sig

def build_text(sym,sig):
    buy=sig["side"]=="buy"
    return "\n".join(["%s %s — ADONIS V7"%("ACHAT" if buy else "VENTE",label(sym)),
      "Fibonacci H4 OTE 62–79 % · CRT H4 sweep + clôture de réintégration",
      "FVG M15 post-réintégration rejeté · entrée clôture M15 : %s"%fmt(sig["entry"]),
      "SL derrière FVG + buffer ATR(14) × %.3f : %s"%(SL_ATR_MULT,fmt(sig["sl"])),
      "TP : %s (%s)"%(fmt(sig["tp"]),sig["tp_label"]),"RR indicatif : %.2f"%sig["rr"],
      "Structure H4, price action CRT et liquidités; aucun ordre n'est passé automatiquement.",
      "Vérifie le prix et le spread sur MT5 avant toute exécution."])


# ==================================================================
#  SCANNER (récupère les bougies et envoie les alertes)
# ==================================================================
class ApiError(Exception):
    pass


_REQ = 0


async def ws_request(ws, payload, timeout=15):
    global _REQ
    _REQ += 1
    rid = _REQ
    await ws.send_str(json.dumps(dict(payload, req_id=rid)))
    deadline = time.time() + timeout
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise asyncio.TimeoutError("pas de réponse de Deriv")
        msg = await ws.receive(timeout=remaining)
        if msg.type == aiohttp.WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
            except ValueError:
                continue
            if data.get("req_id") == rid:
                if data.get("error"):
                    raise ApiError(str(data["error"].get("message", data["error"]))[:150])
                return data
        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
                          aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
            raise ConnectionError("WebSocket fermé par Deriv")


def norm_candles(raw):
    out = []
    for c in raw or []:
        try:
            out.append({"t": int(c["epoch"]), "o": float(c["open"]), "h": float(c["high"]),
                        "l": float(c["low"]), "c": float(c["close"])})
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda x: x["t"])
    return out


async def fetch_candles(ws, sym, gran, count):
    data = await ws_request(ws, {
        "ticks_history": sym, "end": "latest", "style": "candles",
        "granularity": gran, "count": count,
    })
    return norm_candles(data.get("candles"))


async def load_symbols(ws):
    try:
        data = await ws_request(ws, {"active_symbols": "brief"})
    except (ApiError, asyncio.TimeoutError) as e:
        print("active_symbols refusé :", repr(e))
        return
    items = []
    for s in data.get("active_symbols") or []:
        sid = s.get("underlying_symbol") or s.get("symbol")
        if sid:
            items.append([sid, s.get("underlying_symbol_name") or s.get("display_name") or sid])
    items.sort(key=lambda x: x[1])
    STATE["available"] = items
    ids = set(x[0] for x in items)
    for i_, n_ in items:
        NAMES[i_] = n_
    for sym in SYMBOLS:
        if items and sym not in ids:
            hint = HINTS.get(sym, "").lower()
            match = next((i_ for i_, n_ in items if hint and (hint in n_.lower() or hint in i_.lower())), None)
            if match:
                RESOLVED[sym] = match
                print("Marché %s retrouvé par mot-clé : %s" % (sym, match))
            else:
                print("ATTENTION : symbole inconnu chez Deriv :", sym)


async def send_ntfy(title, text):
    if not NTFY_TOPIC:
        return
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as http:
            async with http.post(
                "https://ntfy.sh/" + NTFY_TOPIC,
                data=text.encode("utf-8"),
                headers={"Title": title, "Priority": "high", "Tags": "chart_with_upwards_trend"},
            ) as r:
                if r.status >= 300:
                    print("ntfy a répondu", r.status)
    except Exception as e:
        print("Erreur ntfy :", repr(e))


async def handle_signal(sym, sig):
    key = (sym, sig["side"], sig["c1_t"], sig["fvg_t"])
    if key in SENT:
        return
    if len(SENT) > 500:
        SENT.clear()
    SENT.add(key)
    text = build_text(sym, sig)
    SIGNALS.append({"heure": time.strftime("%H:%M:%S", time.gmtime()) + " UTC", "texte": text})
    print("SIGNAL", sym, sig["side"])
    await send_ntfy("Setup %s %s %s" % (sig.get("grade", "B"), label(sym), "ACHAT" if sig["side"] == "buy" else "VENTE"), text)


def stale(fetched, now, ttl, gran):
    return now - fetched > ttl or int(now // gran) != int(fetched // gran)


async def scan_once(ws, cache):
    for sym in SYMBOLS:
        try:
            now = time.time()
            ck, hk = (sym, 86400), (sym, 14400)
            if ck not in cache or stale(cache[ck][0], now, 900, 86400):
                cache[ck] = (now, await fetch_candles(ws, real(sym), 86400, 150))
            if hk not in cache or stale(cache[hk][0], now, H4_TTL, 14400):
                cache[hk] = (now, await fetch_candles(ws, real(sym), 14400, 300))
            m15 = await fetch_candles(ws, real(sym), 900, 300)
            info, sig = analyze(sym, cache[ck][1], cache[hk][1], m15, time.time())
        except ApiError as e:
            if "rate" in str(e).lower():
                STATE["error"] = "limite de requêtes Deriv atteinte, nouvel essai au prochain tour"
                return
            info, sig = {"resume": "erreur Deriv : %s" % e}, None
        info["label"] = label(sym)
        info["nom"] = NAMES.get(real(sym), "")
        STATE["symbols"][sym] = info
        if sig:
            await handle_signal(sym, sig)
        await asyncio.sleep(0.3)
    STATE["last"] = time.time()


async def scanner_loop():
    await asyncio.sleep(3)
    if _raw_topic and not NTFY_TOPIC:
        print("NTFY_TOPIC invalide : 8 à 64 caractères, lettres, chiffres, - ou _ seulement.")
    print("Scanner : marchés =", SYMBOLS, "| ntfy actif :", bool(NTFY_TOPIC))
    http = None
    ws = None
    cache = {}
    first_ok = True
    while True:
        try:
            if ws is None or ws.closed:
                if http is None or http.closed:
                    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, connect=15))
                ws = await http.ws_connect(PUBLIC_WS, heartbeat=25)
                print("Scanner : connecté au flux public Deriv")
                await load_symbols(ws)
            await scan_once(ws, cache)
            STATE["error"] = None
            if first_ok:
                first_ok = False
                await send_ntfy("Scanner demarre", "Le scanner Adonis tourne sur le serveur. Les alertes arriveront ici.")
        except asyncio.CancelledError:
            if http is not None:
                await http.close()
            raise
        except Exception as e:
            STATE["error"] = (type(e).__name__ + ": " + str(e))[:200]
            print("Scanner erreur :", repr(e))
            try:
                if ws is not None:
                    await ws.close()
            except Exception:
                pass
            ws = None
        await asyncio.sleep(SCAN_EVERY)


# ==================================================================
#  TEST HISTORIQUE : rejoue les règles sur le passé, bougie M15 par bougie M15
#  Règles anti-triche : à chaque instant, l'analyse ne voit que ce qui existait.
#  La bougie H4 « en cours » est reconstruite avec les M15 déjà fermées.
# ==================================================================
def partial_candle(m15, i, T, period):
    p = (T // period) * period
    k = i
    while k >= 0 and m15[k]["t"] >= p:
        k -= 1
    seg = m15[k + 1:i + 1]
    if not seg:
        return None
    return {"t": p, "o": seg[0]["o"], "h": max(x["h"] for x in seg),
            "l": min(x["l"] for x in seg), "c": seg[-1]["c"]}


def simulate(sig, m15, i, max_bars=288):
    """Simulation prudente : les niveaux actifs au début de la bougie sont testés
    avant tout déplacement du trailing. Un SL déplacé à la clôture ne devient
    actif qu'à partir de la bougie suivante. En cas de SL et TP dans la même
    bougie, le SL est retenu (hypothèse conservatrice faute de données tick)."""
    buy = sig["side"] == "buy"
    entry = float(sig["entry"])
    sl = float(sig["sl"])
    tp = float(sig["tp"])
    risk = abs(entry - sl)
    if risk <= 0 or i < 0 or i >= len(m15):
        return "invalid", 0.0, i

    lo = float(sig.get("fvg_bottom", entry))
    hi = float(sig.get("fvg_top", entry))
    armed = False
    zone = None
    last_j = min(len(m15) - 1, i + max_bars)

    def result_for_stop(stop_price):
        r = ((stop_price - entry) / risk) if buy else ((entry - stop_price) / risk)
        label = "win" if r > 1e-12 else ("loss" if r < -1e-12 else "breakeven")
        return label, r

    for j in range(i + 1, last_j + 1):
        c = m15[j]

        # 1) Vérifier uniquement les niveaux déjà actifs avant l'ouverture de
        # cette bougie. SL prioritaire si SL et TP sont tous deux touchés.
        hit_sl = (c["l"] <= sl) if buy else (c["h"] >= sl)
        hit_tp = (c["h"] >= tp) if buy else (c["l"] <= tp)
        if hit_sl:
            label, r = result_for_stop(sl)
            return label, r, j
        if hit_tp:
            r = ((tp - entry) / risk) if buy else ((entry - tp) / risk)
            return ("win" if r > 0 else ("loss" if r < 0 else "breakeven")), r, j

        # 2) Une clôture M15 peut armer ou déplacer le trailing, mais le
        # nouveau SL ne sera testé qu'à partir de la bougie suivante.
        if not armed:
            armed = (c["h"] >= hi) if buy else (c["l"] <= lo)
        if armed and j >= 2:
            a = m15[j - 2]
            if buy and c["l"] > a["h"]:
                zone = (a["h"], c["l"], j)
            elif not buy and c["h"] < a["l"]:
                zone = (c["h"], a["l"], j)
        if zone and j > zone[2]:
            zl, zh, _ = zone
            av = atr(m15[max(0, j - 20):j + 1]) or abs(entry) * 0.0005
            if buy and c["l"] <= zh and c["c"] > zh:
                sl = max(sl, zl - av * SL_ATR_MULT)
                zone = None
            elif not buy and c["h"] >= zl and c["c"] < zl:
                sl = min(sl, zh + av * SL_ATR_MULT)
                zone = None

    close = m15[last_j]["c"]
    r = ((close - entry) if buy else (entry - close)) / risk
    return ("timeout" if i + max_bars < len(m15) else "open"), r, last_j


FUNNEL_RR = 2.0


async def replay(d1, h4, m15, start_i, only=None, mode="fvg", thresholds=None):
    thresholds = tuple(thresholds or (MIN_RR,))
    lo_rr = min(thresholds)
    d1_t = [c["t"] for c in d1]
    h4_t = [c["t"] for c in h4]
    cache = {}
    seen = {t: set() for t in thresholds}
    trades = {t: [] for t in thresholds}
    busy = {t: -1 for t in thresholds}
    st = {"steps": 0, "bias": 0, "zone": 0, "crt": set(), "fvg": set(), "rr": set()}
    for i in range(start_i, len(m15)):
        if i % 200 == 0:
            await asyncio.sleep(0)  # laisse respirer le serveur
        T = m15[i]["t"] + 900
        kd = bisect_right(d1_t, T - 86400)
        kh = bisect_right(h4_t, T - 14400)
        if kd < 15 or kh < 15:
            continue
        key = (kd, kh)
        if key not in cache:
            cache[key] = (None, structure_bias(h4[max(0, kh - 300):kh], SWING_N))
        b_d1, b_h4 = cache[key]
        st["steps"] += 1
        side = pick_side(b_d1, b_h4)
        if side is None or not side_allowed(only, side):
            continue
        st["bias"] += 1
        free = [t for t in thresholds if i > busy[t]]
        if not free:
            continue
        d1c = d1[max(0, kd - 150):kd]
        h4c = h4[max(0, kh - 300):kh]
        m15c = m15[max(0, i + 1 - 300):i + 1]
        h4f = partial_candle(m15, i, T, 14400)
        info, sig = analyze_core(d1c, h4c, h4f, m15c, None, (b_d1, b_h4), only, mode, 0.0)
        rk = RANK.get(info.get("code"), -1)
        if rk >= RANK["no_crt"]:
            st["zone"] += 1
        # Compter un CRT avec sweep uniquement après la détection effective
        # du sweep M15. Le code "no_sweep" signifie précisément qu'il manque.
        if rk > RANK["no_sweep"] and info.get("c1_t"):
            st["crt"].add(info["c1_t"])
        k2 = (info.get("c1_t"), info.get("fvg_t"))
        if rk >= RANK["waiting_retrace"] and k2[0] and k2[1]:
            st["fvg"].add(k2)
        if info.get("code") in ("rr_low", "signal") and k2[0] and k2[1]:
            rr_v = sig["rr"] if sig else info.get("rr")
            if rr_v is None or rr_v < FUNNEL_RR:
                st["rr"].add(k2)
        if not sig:
            continue
        k = (sig["side"], sig["c1_t"], sig["fvg_t"])
        outcome = None
        for thr in free:
            if sig["rr"] >= thr and k not in seen[thr]:
                seen[thr].add(k)
                if outcome is None:
                    outcome = simulate(sig, m15, i)
                res, r, j = outcome
                exit_t = int(m15[j]["t"] + 900) if 0 <= j < len(m15) else None
                trades[thr].append({
                    "sym": None, "side": sig["side"], "t": T,
                    "signal_t": T, "exit_t": exit_t,
                    "res": res, "r": round(float(r), 5), "rr": round(float(sig["rr"]), 5),
                    "entry": sig.get("entry"), "sl": sig.get("sl"), "tp": sig.get("tp"),
                    "tp_label": sig.get("tp_label"), "sweep_t": sig.get("sweep_t"),
                    "crt_t": sig.get("c1_t"), "reentry_t": sig.get("reentry_t"),
                    "reentry_o": sig.get("reentry_o"), "reentry_h": sig.get("reentry_h"),
                    "reentry_l": sig.get("reentry_l"), "reentry_c": sig.get("reentry_c"),
                    "sweep_low": sig.get("sweep_low"), "range_high": sig.get("c1_high"),
                    "range_low": sig.get("c1_low"), "fvg_t": sig.get("fvg_t"),
                    "sweep_depth": (abs(float(sig.get("c1_low")) - float(sig.get("sweep_low")))
                                    if sig.get("c1_low") is not None and sig.get("sweep_low") is not None else None),
                    "range_size": (abs(float(sig.get("c1_high")) - float(sig.get("c1_low")))
                                   if sig.get("c1_high") is not None and sig.get("c1_low") is not None else None),
                    "sweep_depth_ratio": (abs(float(sig.get("c1_low")) - float(sig.get("sweep_low"))) /
                                          abs(float(sig.get("c1_high")) - float(sig.get("c1_low")))
                                          if None not in (sig.get("c1_low"), sig.get("sweep_low"), sig.get("c1_high"))
                                          and abs(float(sig.get("c1_high")) - float(sig.get("c1_low"))) > 0 else None),
                    "reentry_body_ratio": (abs(float(sig.get("reentry_c")) - float(sig.get("reentry_o"))) /
                                           (float(sig.get("reentry_h")) - float(sig.get("reentry_l")))
                                           if None not in (sig.get("reentry_o"), sig.get("reentry_h"), sig.get("reentry_l"), sig.get("reentry_c"))
                                           and float(sig.get("reentry_h")) > float(sig.get("reentry_l")) else None),
                    "reentry_close_location": (((float(sig.get("reentry_c")) - float(sig.get("reentry_l"))) /
                                                 (float(sig.get("reentry_h")) - float(sig.get("reentry_l"))))
                                                if sig.get("side") == "buy" and None not in (sig.get("reentry_c"), sig.get("reentry_l"), sig.get("reentry_h"))
                                                and float(sig.get("reentry_h")) > float(sig.get("reentry_l")) else
                                                (((float(sig.get("reentry_h")) - float(sig.get("reentry_c"))) /
                                                  (float(sig.get("reentry_h")) - float(sig.get("reentry_l"))))
                                                 if sig.get("side") == "sell" and None not in (sig.get("reentry_c"), sig.get("reentry_l"), sig.get("reentry_h"))
                                                 and float(sig.get("reentry_h")) > float(sig.get("reentry_l")) else None)),
                    "bias_d1": b_d1, "bias_h4": b_h4,
                    "hour_utc": time.gmtime(T).tm_hour,
                    "weekday_utc": time.gmtime(T).tm_wday,
                    "bars_held": max(0, j - i) if isinstance(j, int) else None,
                })
                busy[thr] = j
    return trades, st


def performance_stats(trades):
    """Mesures calculées sur les trades clôturés, dans l'ordre chronologique."""
    ordered = sorted((t for t in trades if t["res"] != "open"), key=lambda t: t["t"])
    rs = [float(t["r"]) for t in ordered]
    gross_win = sum(r for r in rs if r > 0)
    gross_loss = -sum(r for r in rs if r < 0)
    equity = peak = max_dd = 0.0
    loss_streak = win_streak = max_loss_streak = max_win_streak = 0
    for r in rs:
        equity += r
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
        if r < 0:
            loss_streak += 1
            win_streak = 0
        elif r > 0:
            win_streak += 1
            loss_streak = 0
        else:
            loss_streak = win_streak = 0
        max_loss_streak = max(max_loss_streak, loss_streak)
        max_win_streak = max(max_win_streak, win_streak)
    n = len(rs)
    return {
        "n_clos": n,
        "expectancy_r": round(sum(rs) / n, 3) if n else 0.0,
        "profit_factor": round(gross_win / gross_loss, 3) if gross_loss else (None if not gross_win else "inf"),
        "max_drawdown_r": round(max_dd, 2),
        "max_pertes_consecutives": max_loss_streak,
        "max_gains_consecutifs": max_win_streak,
        "sans_best1_r": round(sum(rs) - sum(sorted(rs, reverse=True)[:1]), 2),
        "sans_best2_r": round(sum(rs) - sum(sorted(rs, reverse=True)[:2]), 2),
        "sans_best3_r": round(sum(rs) - sum(sorted(rs, reverse=True)[:3]), 2),
        "sans_best5_r": round(sum(rs) - sum(sorted(rs, reverse=True)[:5]), 2),
    }


def summarize(sym, trades):
    closed = [t for t in trades if t["res"] != "open"]
    w = sum(1 for t in trades if t["res"] == "win")
    l = sum(1 for t in trades if t["res"] == "loss")
    be = sum(1 for t in trades if t["res"] == "breakeven")
    to = sum(1 for t in trades if t["res"] == "timeout")
    op = sum(1 for t in trades if t["res"] == "open")
    total_r = sum(t["r"] for t in closed)
    buys = [t for t in closed if t["side"] == "buy"]
    sells = [t for t in closed if t["side"] == "sell"]
    stats = performance_stats(closed)
    buy_stats = performance_stats(buys)
    sell_stats = performance_stats(sells)
    return {"symbole": label(sym), "achats": len(buys), "r_achats": round(sum(t["r"] for t in buys), 2),
            "achats_pf": buy_stats["profit_factor"], "achats_dd_r": buy_stats["max_drawdown_r"],
            "achats_expectancy_r": buy_stats["expectancy_r"],
            "ventes": len(sells), "r_ventes": round(sum(t["r"] for t in sells), 2),
            "ventes_pf": sell_stats["profit_factor"], "ventes_dd_r": sell_stats["max_drawdown_r"],
            "ventes_expectancy_r": sell_stats["expectancy_r"], "setups": len(trades),
            "gagnes": w, "perdus": l, "equilibres": be, "expires": to, "ouverts": op, "r_total": round(total_r, 2),
            "profit_factor": stats["profit_factor"], "max_drawdown_r": stats["max_drawdown_r"],
            "expectancy_r": stats["expectancy_r"]}


def combo_total(trades, window_start, window_end):
    split_t = window_end - (window_end - window_start) / 3
    # Les trades encore ouverts ne sont pas inclus dans les performances réalisées.
    closed = sorted((t for t in trades if t["res"] != "open"), key=lambda t: t["t"])
    old = [t for t in closed if t["t"] < split_t]
    new = [t for t in closed if t["t"] >= split_t]
    total_r = sum(t["r"] for t in closed)
    stats = performance_stats(closed)

    # Trois périodes calendaires fixes sur la fenêtre complète du backtest,
    # y compris les périodes sans aucun trade.
    periods = []
    span = max(1, window_end - window_start)
    for idx in range(3):
        lo = window_start + span * idx / 3
        hi = window_start + span * (idx + 1) / 3
        group = [t for t in closed if lo <= t["t"] < hi or (idx == 2 and t["t"] == window_end)]
        periods.append({"n": len(group), "r": round(sum(t["r"] for t in group), 2)})

    buys = performance_stats([t for t in closed if t["side"] == "buy"])
    sells = performance_stats([t for t in closed if t["side"] == "sell"])
    return {"setups": len(trades),
            "gagnes": sum(1 for t in trades if t["res"] == "win"),
            "perdus": sum(1 for t in trades if t["res"] == "loss"),
            "equilibres": sum(1 for t in trades if t["res"] == "breakeven"),
            "expires": sum(1 for t in trades if t["res"] == "timeout"),
            "ouverts": sum(1 for t in trades if t["res"] == "open"),
            "r_total": round(total_r, 2),
            "n_old": len(old), "r_old": round(sum(t["r"] for t in old), 2),
            "n_new": len(new), "r_new": round(sum(t["r"] for t in new), 2),
            "periods": periods,
            "profit_factor": stats["profit_factor"],
            "expectancy_r": stats["expectancy_r"],
            "max_drawdown_r": stats["max_drawdown_r"],
            "max_pertes_consecutives": stats["max_pertes_consecutives"],
            "max_gains_consecutifs": stats["max_gains_consecutifs"],
            "sans_best1_r": stats["sans_best1_r"],
            "sans_best2_r": stats["sans_best2_r"],
            "sans_best3_r": stats["sans_best3_r"],
            "sans_best5_r": stats["sans_best5_r"],
            "achats_pf": buys["profit_factor"], "ventes_pf": sells["profit_factor"],
            "achats_r": round(sum(t["r"] for t in closed if t["side"] == "buy"), 2),
            "ventes_r": round(sum(t["r"] for t in closed if t["side"] == "sell"), 2)}


async def hist_request(ws, payload):
    for attempt in range(5):
        try:
            return await ws_request(ws, payload, timeout=40)
        except ApiError as e:
            if "rate" in str(e).lower() and attempt < 4:
                BT["progress"] = "limite de Deriv atteinte, pause de 45 s..."
                await asyncio.sleep(45)
                continue
            raise


async def fetch_history(ws, sym, gran, target, page):
    got = {}
    end = "latest"
    for _ in range(8):
        data = await hist_request(ws, {"ticks_history": sym, "end": end, "style": "candles",
                                       "granularity": gran, "count": page})
        cs = norm_candles(data.get("candles"))
        fresh = [c for c in cs if c["t"] not in got]
        for c in cs:
            got[c["t"]] = c
        if not cs or len(got) >= target or len(fresh) <= 1:
            break
        end = str(cs[0]["t"])
        await asyncio.sleep(2.5)
    await asyncio.sleep(2.5)
    return sorted(got.values(), key=lambda c: c["t"])


BT_MODES = ("fvg",)
BT_THRS = (0.0, 1.0, 1.5, 2.0)


def trade_audit_groups(trades, key_fields):
    """Regroupe les trades pour rechercher des causes récurrentes sans masquer les cas individuels."""
    buckets = {}
    for trade in trades:
        key = tuple(trade.get(k) for k in key_fields)
        buckets.setdefault(key, []).append(trade)
    out = []
    for key, group in sorted(buckets.items(), key=lambda item: tuple(str(x) for x in item[0])):
        closed = [t for t in group if t.get("res") != "open"]
        wins = sum(t.get("res") == "win" for t in group)
        losses = sum(t.get("res") == "loss" for t in group)
        breakevens = sum(t.get("res") == "breakeven" for t in group)
        total_r = round(sum(float(t.get("r", 0) or 0) for t in closed), 3)
        gross_win = sum(float(t.get("r", 0) or 0) for t in closed if float(t.get("r", 0) or 0) > 0)
        gross_loss = -sum(float(t.get("r", 0) or 0) for t in closed if float(t.get("r", 0) or 0) < 0)
        out.append({**dict(zip(key_fields, key)), "trades": len(group), "gagnes": wins,
                    "perdus": losses, "equilibres": breakevens, "ouverts": sum(t.get("res") == "open" for t in group),
                    "r_net": total_r, "esperance_r": round(total_r / len(closed), 3) if closed else 0.0,
                    "pf": round(gross_win / gross_loss, 3) if gross_loss else ("inf" if gross_win else None)})
    return out


def history_coverage(candles, granularity, window_start, window_end):
    """Couverture réelle dans la fenêtre, trous et chevauchement observés."""
    inside = [c for c in candles if window_start <= int(c["t"]) < window_end]
    if not inside:
        return {"n": 0, "debut": None, "fin_cloture": None,
                "couverture_pct": 0.0, "trous": 0, "plus_grand_trou_bougies": 0}
    ts = sorted(set(int(c["t"]) for c in inside))
    expected = max(1, int((window_end - window_start) // granularity))
    missing = []
    for a, b in zip(ts, ts[1:]):
        gap = max(0, int((b - a) // granularity) - 1)
        if gap:
            missing.append(gap)
    return {"n": len(ts), "debut": ts[0], "fin_cloture": ts[-1] + granularity,
            "couverture_pct": round(min(100.0, len(ts) / expected * 100.0), 1),
            "trous": len(missing), "plus_grand_trou_bougies": max(missing) if missing else 0}


def _closed_segment(trades, start_t, end_t):
    return [t for t in trades if t.get("res") != "open" and start_t <= int(t.get("t", 0)) < end_t]


def _stats_adjusted(trades, costs):
    adjusted = []
    total_cost = 0.0
    for t in trades:
        if t.get("res") == "open":
            continue
        row = dict(t)
        cost = costs(t) if callable(costs) else float(costs)
        row["r"] = float(t.get("r", 0) or 0) - cost
        total_cost += cost
        adjusted.append(row)
    stats = performance_stats(adjusted)
    stats.update({"trades": len(adjusted), "r_net": round(sum(float(t.get("r", 0) or 0) for t in adjusted), 3),
                  "cout_total_r": round(total_cost, 3)})
    return stats


def _load_cost_config():
    try:
        default_cost = max(0.0, float(os.environ.get("BT_COST_R", "0")))
    except (TypeError, ValueError):
        default_cost = 0.0
    try:
        raw = json.loads(os.environ.get("BT_COST_R_BY_SYMBOL", "{}"))
        by_symbol = {str(k): max(0.0, float(v)) for k, v in raw.items()} if isinstance(raw, dict) else {}
    except (ValueError, TypeError, AttributeError):
        by_symbol = {}
    return default_cost, by_symbol


def _chronological_validation(trades, window_start, window_end):
    split_t = int(window_start + (window_end - window_start) * 2 / 3)
    dev = _closed_segment(trades, window_start, split_t)
    holdout = _closed_segment(trades, split_t, window_end)
    return {"date_separation_utc": split_t,
            "developpement_premiers_2_3": _stats_adjusted(dev, 0.0),
            "hors_echantillon_dernier_tiers": _stats_adjusted(holdout, 0.0),
            "note": "Découpage chronologique strict. Le dernier tiers est réservé à la validation et ne sert pas à choisir les règles."}


def _cost_sensitivity(trades, window_start, window_end, default_cost, by_symbol):
    split_t = int(window_start + (window_end - window_start) * 2 / 3)
    holdout = _closed_segment(trades, split_t, window_end)
    configured = _stats_adjusted(holdout, lambda t: by_symbol.get(t.get("sym"), default_cost))
    stress = []
    for cost in (0.02, 0.05, 0.10):
        st = _stats_adjusted(holdout, cost)
        stress.append({"cout_r_par_trade": cost, **st})
    return {"periode": "dernier_tiers_hors_echantillon", "couts_configures": configured,
            "cout_par_defaut_r": default_cost, "couts_par_marche_r": by_symbol,
            "scenarios_stress": stress,
            "avertissement": "Les scénarios 0.02/0.05/0.10 R sont des sensibilités, pas des spreads mesurés. Les coûts réalistes doivent être calibrés avec spread, commission et slippage observés par marché."}


def _quality_class(value, a, b, labels):
    if value is None:
        return "inconnu"
    return labels[0] if value < a else (labels[1] if value < b else labels[2])


def enrich_trade_quality(trades):
    for t in trades:
        t["sweep_depth_classe"] = _quality_class(t.get("sweep_depth_ratio"), 0.05, 0.15,
                                                 ("faible", "moyen", "profond"))
        t["reentry_body_classe"] = _quality_class(t.get("reentry_body_ratio"), 0.25, 0.60,
                                                  ("petit_corps", "corps_moyen", "grand_corps"))
        loc = t.get("reentry_close_location")
        t["reentry_close_classe"] = "inconnu" if loc is None else ("cloture_defavorable" if loc < 0.5 else "cloture_favorable")


async def run_backtest():
    BT.update(running=True, done=False, error=None, results=None, progress="connexion à Deriv...")
    http = None
    ws = None
    try:
        http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, connect=15))
        ws = await http.ws_connect(PUBLIC_WS, heartbeat=25)
        rows = {(m, t): [] for m in BT_MODES for t in BT_THRS}
        alltr = {(m, t): [] for m in BT_MODES for t in BT_THRS}
        funnel = {m: {"steps": 0, "bias": 0, "zone": 0, "crt": 0, "fvg": 0, "rr": 0} for m in BT_MODES}
        split_t = 0
        # Fenêtre UTC figée une seule fois au lancement : aucune donnée future ne peut entrer.
        window_end = int(time.time() // 900) * 900
        window_start = window_end - BT_DAYS * 86400
        n_ok = 0
        coverage = {}
        for n, sym in enumerate(SYMBOLS):
            tag = "%d/%d %s" % (n + 1, len(SYMBOLS), sym)
            BT["progress"] = tag + " : téléchargement des bougies..."
            err = None
            try:
                d1_raw = await fetch_history(ws, real(sym), 86400, 400, 400)
                h4_raw = await fetch_history(ws, real(sym), 14400, 1500, 1500)
                m15_raw = await fetch_history(ws, real(sym), 900, (BT_DAYS + 4) * 96, 4000)
                # Toutes les unités sont arrêtées à LA MÊME borne fixe du backtest.
                # Une bougie est admissible uniquement si sa clôture est <= window_end.
                d1 = [c for c in d1_raw if int(c["t"]) + 86400 <= window_end]
                h4 = [c for c in h4_raw if int(c["t"]) + 14400 <= window_end]
                m15 = [c for c in m15_raw if int(c["t"]) + 900 <= window_end]
                coverage[sym] = {
                    "D1": history_coverage(d1_raw, 86400, window_start, window_end),
                    "H4": history_coverage(h4_raw, 14400, window_start, window_end),
                    "M15": history_coverage(m15, 900, window_start, window_end),
                    "M15_warmup_avant_debut": sum(1 for c in m15 if int(c["t"]) < window_start),
                }
                if len(m15) < 400:
                    err = "pas assez d'historique M15 clôturé avant la borne fixe (%d bougies)" % len(m15)
            except ApiError as e:
                err = str(e)
            if err:
                for k in rows:
                    rows[k].append({"symbole": label(sym), "erreur": err})
                continue
            n_ok += 1
            split_t = window_end - (BT_DAYS // 3) * 86400
            start_t = window_start
            start_i = next((i for i, c in enumerate(m15) if c["t"] >= start_t), None)
            if start_i is None:
                coverage[sym]["erreur_fenetre"] = "aucune bougie M15 dans la fenêtre demandée"
                for k in rows:
                    rows[k].append({"symbole": label(sym), "erreur": coverage[sym]["erreur_fenetre"]})
                n_ok -= 1
                continue
            coverage[sym]["premiere_bougie_analysee"] = int(m15[start_i]["t"])
            coverage[sym]["derniere_bougie_analysee_cloture"] = int(m15[-1]["t"] + 900) if m15 else None
            coverage[sym]["retard_debut_jours"] = round(max(0, int(m15[start_i]["t"]) - window_start) / 86400, 2)
            for mode in BT_MODES:
                BT["progress"] = tag + " : analyse (" + mode + ")..."
                trs, st = await replay(d1, h4, m15, start_i, SIDE_ONLY.get(sym), mode, BT_THRS)
                for thr in BT_THRS:
                    rows[(mode, thr)].append(summarize(sym, trs[thr]))
                    for trade in trs[thr]:
                        trade["sym"] = sym
                    alltr[(mode, thr)].extend(trs[thr])
                f = funnel[mode]
                f["steps"] += st["steps"]
                f["bias"] += st["bias"]
                f["zone"] += st["zone"]
                f["crt"] += len(st["crt"])
                f["fvg"] += len(st["fvg"])
                f["rr"] += len(st["rr"])
        refresh_market_history(alltr)
        default_cost_r, cost_by_symbol = _load_cost_config()
        combos = []
        for mode in BT_MODES:
            for thr in BT_THRS:
                combo_trades = alltr[(mode, thr)]
                combo_rows = rows[(mode, thr)]
                aggregate = combo_total(combo_trades, window_start, window_end)
                row_setups = sum(int(x.get("setups", 0)) for x in combo_rows if "erreur" not in x)
                outcome_sum = aggregate["gagnes"] + aggregate["perdus"] + aggregate["equilibres"] + aggregate["expires"] + aggregate["ouverts"]
                combos.append({"mode": mode, "min_rr": thr, "total": aggregate,
                               "validation_chronologique": _chronological_validation(combo_trades, window_start, window_end),
                               "sensibilite_couts_hors_echantillon": _cost_sensitivity(combo_trades, window_start, window_end, default_cost_r, cost_by_symbol),
                               "reconciliation": {"trades_internes": len(combo_trades),
                                                  "setups_lignes_marches": row_setups,
                                                  "somme_resultats": outcome_sum,
                                                  "ecart_trades_vs_lignes": len(combo_trades) - row_setups,
                                                  "ecart_resultats_vs_setups": len(combo_trades) - outcome_sum},
                               "lignes": combo_rows})
        primary_audit = alltr[("fvg", 0.0)]
        enrich_trade_quality(primary_audit)
        losses_audit = [t for t in primary_audit if t.get("res") == "loss"]
        BT["results"] = {"version": "V7", "strategie_reference": "Fibonacci H4 OTE 62-79; CRT H4; FVG M15 rejet; SL ATR; TP liquidité H4/D1; trailing FVG M15",
                         "jours": BT_DAYS, "jours_new": BT_DAYS // 3,
                         "fenetre_debut_utc": window_start, "fenetre_fin_utc": window_end,
                         "bougies_m15_incompletes_exclues": True, "borne_commune_toutes_unites": True,
                         "simulateur_sorties": "SL/TP actifs testés avant trailing; nouveau SL actif à la bougie suivante; SL prioritaire si SL et TP touchés dans la même M15; sorties breakeven séparées",
                         "marches": n_ok, "couverture_historique": coverage,
                         "audit_trades_v7": primary_audit,
                         "audit_pertes_v7": losses_audit,
                         "audit_par_marche": trade_audit_groups(primary_audit, ("sym",)),
                         "audit_par_marche_direction": trade_audit_groups(primary_audit, ("sym", "side")),
                         "audit_par_heure_utc": trade_audit_groups(primary_audit, ("hour_utc",)),
                         "audit_par_biais": trade_audit_groups(primary_audit, ("bias_d1", "bias_h4")),
                         "audit_par_profondeur_sweep": trade_audit_groups(primary_audit, ("sweep_depth_classe",)),
                         "audit_par_qualite_reintegration": trade_audit_groups(primary_audit, ("reentry_body_classe", "reentry_close_classe")),
                         "mode_validation": "premiers 2/3 développement; dernier tiers hors échantillon chronologique",
                         "couts_reels_verifies": False,
                         "couts_configures": {"BT_COST_R": default_cost_r, "BT_COST_R_BY_SYMBOL": cost_by_symbol},
                         "note_couts": "Sensibilités incluses; les coûts réels doivent être calibrés par marché avec spread, commission et slippage observés.",
                         "entonnoir": funnel, "combos": combos}
        BT["done"] = True
        BT["progress"] = "terminé"
    except asyncio.CancelledError:
        raise
    except Exception as e:
        BT["error"] = (type(e).__name__ + ": " + str(e))[:200]
        print("Test historique erreur :", repr(e))
    finally:
        BT["running"] = False
        try:
            if ws is not None:
                await ws.close()
            if http is not None:
                await http.close()
        except Exception:
            pass


async def on_startup(app):
    app["scanner"] = asyncio.create_task(scanner_loop())


async def on_cleanup(app):
    task = app.get("scanner")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ------------------------------------------------------------------
# OUTILS
# ------------------------------------------------------------------
def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def cleanup():
    now = time.time()
    for k in [k for k, v in PENDING.items() if now - v["ts"] > 600]:
        PENDING.pop(k, None)
    for k in [k for k, v in SESSIONS.items() if v["exp"] < now]:
        SESSIONS.pop(k, None)


def get_session(request):
    sid = request.cookies.get("sid", "")
    s = SESSIONS.get(sid)
    if s and s["exp"] > time.time():
        return sid, s
    return None, None


def error_page(message, status=400):
    body = (
        "<body style='font-family:sans-serif;background:#0f1117;color:#f0f0f0;padding:20px'>"
        "<h3>Erreur</h3><p>" + html.escape(message) + "</p>"
        "<p><a style='color:#38bdf8' href='/'>Retour</a></p></body>"
    )
    return web.Response(text=body, status=status, content_type="text/html")


def parse_json(text):
    try:
        return json.loads(text)
    except ValueError:
        return {"raw": text[:500]}


# ------------------------------------------------------------------
# PAGES
# ------------------------------------------------------------------
async def index(request):
    return web.Response(
        text=HTML_CONTENT,
        content_type="text/html",
        headers={"Cache-Control": "no-store"},
    )


async def login(request):
    if not APP_ID:
        return error_page("DERIV_APP_ID est vide. Ajoute-le dans Railway > Variables.", 500)
    cleanup()
    verifier = b64url(secrets.token_bytes(48))
    challenge = b64url(hashlib.sha256(verifier.encode()).digest())
    state = secrets.token_urlsafe(24)
    PENDING[state] = {"verifier": verifier, "ts": time.time()}

    params = {
        "response_type": "code",
        "client_id": APP_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": "trade",
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    resp = web.Response(status=302, headers={"Location": AUTH_URL + "?" + urlencode(params)})
    resp.set_cookie("oauth_state", state, max_age=600, httponly=True, secure=True, samesite="Lax")
    return resp


async def callback(request):
    q = request.query
    if "error" in q:
        return error_page("Deriv a refusé : " + q.get("error", "") + " " + q.get("error_description", ""))

    state = q.get("state", "")
    code = q.get("code", "")
    pending = PENDING.pop(state, None)
    if not pending or not code or state != request.cookies.get("oauth_state"):
        return error_page("Vérification de sécurité échouée (state). Recommence la connexion.")

    form = {
        "grant_type": "authorization_code",
        "client_id": APP_ID,
        "code": code,
        "code_verifier": pending["verifier"],
        "redirect_uri": REDIRECT_URI,
    }
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as http:
            async with http.post(TOKEN_URL, data=form) as r:
                status = r.status
                text = await r.text()
    except Exception as e:
        print("Erreur échange token :", repr(e))
        return error_page("Impossible de joindre Deriv pour l'échange du token : " + str(e), 502)

    data = parse_json(text)
    if status != 200 or not isinstance(data, dict) or "access_token" not in data:
        print("Échange token refusé :", status, text[:300])
        return error_page("Deriv a refusé l'échange du token (code " + str(status) + "). Regarde les logs Railway.", 502)

    try:
        expires_in = int(data.get("expires_in", 3600))
    except (TypeError, ValueError):
        expires_in = 3600

    sid = secrets.token_urlsafe(32)
    SESSIONS[sid] = {"token": data["access_token"], "exp": time.time() + expires_in}

    resp = web.Response(status=302, headers={"Location": "/"})
    resp.set_cookie("sid", sid, max_age=min(expires_in, 7 * 86400), httponly=True, secure=True, samesite="Lax")
    resp.del_cookie("oauth_state")
    return resp


async def logout(request):
    sid, _ = get_session(request)
    if sid:
        SESSIONS.pop(sid, None)
    resp = web.Response(status=302, headers={"Location": "/"})
    resp.del_cookie("sid")
    return resp


# ------------------------------------------------------------------
# API INTERNE (appelée par la page)
# ------------------------------------------------------------------
async def api_me(request):
    _, s = get_session(request)
    return web.json_response({"logged_in": bool(s), "app_id_ok": bool(APP_ID)})


def ntfy_etat():
    if NTFY_TOPIC:
        return "actives"
    if _raw_topic:
        return "NOM INVALIDE (lettres, chiffres, - et _ seulement, 8 à 64 caractères)"
    return "variable NTFY_TOPIC introuvable sur le serveur"


async def api_scanner(request):
    _, s = get_session(request)
    if not s:
        return web.json_response({"error": "not_logged_in"}, status=401)
    return web.json_response({
        "derniere_analyse": STATE["last"],
        "erreur": STATE["error"],
        "ntfy": bool(NTFY_TOPIC),
        "ntfy_etat": ntfy_etat(),
        "symboles": STATE["symbols"],
        "signaux": list(SIGNALS)[::-1],
        "disponibles": STATE["available"][:300],
    })


async def api_bt_start(request):
    _, s = get_session(request)
    if not s:
        return web.json_response({"error": "not_logged_in"}, status=401)
    if not BT["running"]:
        request.app["bt_task"] = asyncio.create_task(run_backtest())
    return web.json_response({"running": True})


async def api_bt_status(request):
    _, s = get_session(request)
    if not s:
        return web.json_response({"error": "not_logged_in"}, status=401)
    return web.json_response(BT)


def deriv_headers(token):
    return {"Authorization": "Bearer " + token, "Deriv-App-ID": APP_ID}


async def api_accounts(request):
    sid, s = get_session(request)
    if not s:
        return web.json_response({"error": "not_logged_in"}, status=401)
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as http:
            async with http.get(API_BASE + "/trading/v1/options/accounts", headers=deriv_headers(s["token"])) as r:
                status = r.status
                text = await r.text()
    except Exception as e:
        print("Erreur liste comptes :", repr(e))
        return web.json_response({"error": "network", "detail": str(e)}, status=502)
    if status == 401:
        SESSIONS.pop(sid, None)
    return web.json_response({"status": status, "body": parse_json(text)})


async def api_otp(request):
    _, s = get_session(request)
    if not s:
        return web.json_response({"error": "not_logged_in"}, status=401)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    account_id = str(payload.get("account_id", ""))
    if not ACCOUNT_RE.match(account_id):
        return web.json_response({"error": "account_id invalide"}, status=400)

    url = API_BASE + "/trading/v1/options/accounts/" + account_id + "/otp"
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as http:
            async with http.post(url, headers=deriv_headers(s["token"])) as r:
                status = r.status
                text = await r.text()
    except Exception as e:
        print("Erreur OTP :", repr(e))
        return web.json_response({"error": "network", "detail": str(e)}, status=502)

    body = parse_json(text)
    ws_url = None
    if isinstance(body, dict) and isinstance(body.get("data"), dict):
        ws_url = body["data"].get("url")
    if 200 <= status < 300 and ws_url:
        return web.json_response({"url": ws_url})
    print("OTP refusé :", status, text[:300])
    return web.json_response({"error": "otp_failed", "status": status, "body": body}, status=502)


# ------------------------------------------------------------------
# DÉMARRAGE
# ------------------------------------------------------------------
app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/login", login)
app.router.add_get("/callback", callback)
app.router.add_get("/logout", logout)
app.router.add_get("/api/me", api_me)
app.router.add_get("/api/scanner", api_scanner)
app.router.add_post("/api/backtest/start", api_bt_start)
app.router.add_get("/api/backtest", api_bt_status)
app.router.add_get("/api/accounts", api_accounts)
app.router.add_post("/api/otp", api_otp)
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)

if __name__ == "__main__":
    print("DERIV_APP_ID défini :", bool(APP_ID))
    print("REDIRECT_URI :", REDIRECT_URI)
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host="0.0.0.0", port=port)
