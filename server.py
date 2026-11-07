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
  ["fvg", "close"].forEach(mode => {
    const f = res.entonnoir[mode];
    const c2 = res.combos.find(c => c.mode === mode && c.min_rr === 2);
    const d = document.createElement("div");
    d.className = "log-item";
    d.textContent = MODE_NAMES[mode] + " — entonnoir (" + res.marches + " marchés, " + res.jours + " jours) : biais H4 exploitable " + pct(f.bias, f.steps) + " % du temps · bon côté de l'équilibre " + pct(f.zone, f.steps) + " % · CRT avec sweep : " + f.crt + " · entrées possibles : " + f.fvg + " · gain/risque < 2 : " + f.rr + " · signaux (RR ≥ 2) : " + (c2 ? c2.total.setups : 0);
    list.appendChild(d);
  });
  res.combos.forEach(c => {
    const t = c.total;
    const done = t.gagnes + t.perdus + t.expires;
    const per = done ? (t.r_total / done).toFixed(2) : "0.00";
    const det = document.createElement("details");
    det.className = "log-item";
    det.style.borderLeftColor = t.r_total > 0 ? "#22c55e" : "#ef4444";
    const sm = document.createElement("summary");
    sm.textContent = MODE_NAMES[c.mode] + " · RR ≥ " + c.min_rr + " : " + t.setups + " trades · " + t.gagnes + " gagnés · " + t.perdus + " perdus · " + sign(t.r_total) + " R (" + sign(per) + " R/trade) · avant les " + res.jours_new + " derniers jours : " + t.n_old + " trades, " + sign(t.r_old) + " R · " + res.jours_new + " derniers jours : " + t.n_new + " trades, " + sign(t.r_new) + " R";
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
    """V7 trailing : après contact du bord directionnel du FVG initial,
    un nouveau FVG M15 rejeté déplace le SL. SL prioritaire en cas d'ambiguïté."""
    buy=sig["side"]=="buy"; entry=float(sig["entry"]); sl=float(sig["sl"]); tp=float(sig["tp"])
    risk=abs(entry-sl)
    if risk<=0 or i<0 or i>=len(m15): return "invalid",0.0,i
    lo=float(sig.get("fvg_bottom",entry)); hi=float(sig.get("fvg_top",entry))
    armed=False; zone=None; last_j=min(len(m15)-1,i+max_bars)
    for j in range(i+1,last_j+1):
        c=m15[j]
        if not armed: armed=(c["h"]>=hi) if buy else (c["l"]<=lo)
        if armed and j>=2:
            a=m15[j-2]
            if buy and c["l"]>a["h"]: zone=(a["h"],c["l"],j)
            elif not buy and c["h"]<a["l"]: zone=(c["h"],a["l"],j)
        if zone and j>zone[2]:
            zl,zh,_=zone
            av=atr(m15[max(0,j-20):j+1]) or abs(entry)*0.0005
            if buy and c["l"]<=zh and c["c"]>zh:
                sl=max(sl,zl-av*SL_ATR_MULT); zone=None
            elif not buy and c["h"]>=zl and c["c"]<zl:
                sl=min(sl,zh+av*SL_ATR_MULT); zone=None
        hit_sl=(c["l"]<=sl) if buy else (c["h"]>=sl)
        hit_tp=(c["h"]>=tp) if buy else (c["l"]<=tp)
        if hit_sl: return "loss",((sl-entry)/risk if buy else (entry-sl)/risk),j
        if hit_tp: return "win",((tp-entry)/risk if buy else (entry-tp)/risk),j
    close=m15[last_j]["c"]; r=((close-entry) if buy else (entry-close))/risk
    return ("timeout" if i+max_bars<len(m15) else "open"),r,last_j

