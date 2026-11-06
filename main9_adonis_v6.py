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
ENTRY_MODE = os.environ.get("ENTRY_MODE", "fvg").lower()  # V6 impose le premier FVG M15 confirmé + retest strict
if ENTRY_MODE != "fvg":
    ENTRY_MODE = "fvg"
PD_RANGE = os.environ.get("PD_RANGE", "H4").upper()    # range du discount/premium : H4 ou D1
SWING_N = int(os.environ.get("SWING_N", "2"))        # bougies de chaque côté d'un sommet/creux
MIN_RR = float(os.environ.get("MIN_RR", "2.0"))       # gain/risque minimum
EQ_TOL = float(os.environ.get("EQ_TOL", "0.0005"))   # tolérance des Equal Highs/Lows (0.05 %)
SCAN_EVERY = int(os.environ.get("SCAN_EVERY", "60"))  # secondes entre deux analyses
H4_TTL = 240                                          # secondes avant de retélécharger le H4
BT_DAYS = int(os.environ.get("BT_DAYS", "180"))        # durée du test historique (jours)
# V6: buffer configurable (valeurs à calibrer, non optimisées).
V6_BUFFER_MODE = os.environ.get("V6_BUFFER_MODE", "atr").lower()  # fixed ou atr
V6_FIXED_BUFFER = float(os.environ.get("V6_FIXED_BUFFER", "0"))
V6_ATR_PERIOD = int(os.environ.get("V6_ATR_PERIOD", "14"))
V6_ATR_MULT = float(os.environ.get("V6_ATR_MULT", "0.10"))
V6_RUNNER_PROTECTION = os.environ.get("V6_RUNNER_PROTECTION", "fvg").lower()  # fvg ou breakeven

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
<title>Adonis V6 - Terminal Pro</title>
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
  ["fvg"].forEach(mode => {
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
    const wk = t.activite_hebdomadaire || {};
    const det = document.createElement("details");
    det.className = "log-item";
    det.style.borderLeftColor = t.r_total > 0 ? "#22c55e" : "#ef4444";
    const sm = document.createElement("summary");
    sm.textContent = MODE_NAMES[c.mode] + " · RR ≥ " + c.min_rr + " : " + t.setups + " trades · " + t.gagnes + " gagnés · " + t.perdus + " perdus · " + sign(t.r_total) + " R (" + sign(per) + " R/trade) · avant les " + res.jours_new + " derniers jours : " + t.n_old + " trades, " + sign(t.r_old) + " R · " + res.jours_new + " derniers jours : " + t.n_new + " trades, " + sign(t.r_new) + " R · moyenne " + (wk.moyenne_trades_semaine || 0) + " trade(s)/semaine · " + (wk.semaines_avec_trade || 0) + "/" + (wk.semaines_total || 0) + " semaines actives";
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
  note.textContent = "V6 : premier FVG M15 verrouillé, retest strict, SL derrière le FVG, sortie partielle 50/50 H4/D1 et trailing FVG. Le dernier tiers est réservé au hors-échantillon. Sensibilité aux coûts de 0,02 / 0,05 / 0,10 R par trade (scénarios, pas spreads observés). Configure BT_COST_R et BT_COST_R_BY_SYMBOL avec des coûts vérifiés. Petit échantillon = conclusion fragile.";
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
RANK = {
    "no_range": 0, "not_in_zone": 1, "no_crt": 2, "no_sweep": 3,
    "no_reentry": 4, "no_fvg": 5, "waiting_retrace": 6, "rr_low": 7, "signal": 8,

    "fvg_invalidated": 5,
}
ARROW = {"up": "↑", "down": "↓", None: "?"}

# Profils historiques issus du backtest CRT clôture M15 (180 jours, RR >= 2).
# Une donnée absente n'est PAS considérée comme une preuve de rentabilité.
# USDJPY est le seul marché clairement défavorable dans les résultats communiqués :
# les achats comme les ventes sont négatifs, et les ventes RR>=2 ont fait 6 pertes/6.
# Les autres marchés restent à confirmer par un échantillon directionnel suffisant.
MARKET_HISTORY = {
    "frxUSDJPY": {"buy": {"status": "weak", "note": "historique négatif dans les trois seuils RR testés"},
                  "sell": {"status": "weak", "note": "6 ventes RR>=2, 6 pertes (-6R) ; historique négatif"}},
}
HISTORY_MIN_TRADES = 20
HISTORY_MIN_PF = 1.25
HISTORY_MIN_EXPECTANCY = 0.15


def classify_setup(sym, sig):
    """Classe le signal sans confondre qualité technique et preuve statistique."""
    side = sig["side"]
    rr = float(sig.get("rr", 0.0) or 0.0)
    hist = MARKET_HISTORY.get(sym, {}).get(side)

    # Qualité technique : le CRT, le sweep et la clôture M15 de réintégration
    # sont déjà obligatoires pour qu'un signal soit produit. Le RR départage ici.
    if rr < 2.0:
        technical = "B"
    elif rr >= 3.5:
        technical = "A+"
    else:
        technical = "A"

    # A+ exige en plus un historique directionnel robuste, pas seulement un beau RR.
    if hist and hist.get("status") == "weak":
        return "B", "marché/sens historiquement défavorable : " + hist.get("note", "prudence")

    if hist and hist.get("status") == "validated":
        n = int(hist.get("n", 0))
        pf = hist.get("pf")
        expectancy = hist.get("expectancy", 0.0)
        if (n >= HISTORY_MIN_TRADES and pf is not None and pf >= HISTORY_MIN_PF
                and expectancy >= HISTORY_MIN_EXPECTANCY and technical == "A+"):
            return "A+", "historique directionnel validé (%d trades, PF %s, espérance %sR)" % (
                n, pf, expectancy)

    if technical == "B":
        return "B", "qualité technique insuffisante"
    if hist and hist.get("status") == "promising":
        return "A", "historique prometteur mais échantillon trop faible pour A+"
    return "A", "technique propre, mais historique directionnel pas encore validé pour A+"


def refresh_market_history(alltr):
    """Actualise les profils directionnels depuis le backtest CRT clôture RR>=2."""
    global MARKET_HISTORY
    trades = alltr.get(("fvg", 2.0), [])
    refreshed = dict(MARKET_HISTORY)
    symbols = sorted({t.get("sym") for t in trades if t.get("sym")})
    for sym in symbols:
        per_side = dict(refreshed.get(sym, {}))
        for side in ("buy", "sell"):
            group = [t for t in trades if t.get("sym") == sym and t.get("side") == side
                     and t.get("res") != "open"]
            if not group:
                continue
            stats = performance_stats(group)
            n = stats["n_clos"]
            # Ne remplace pas un avertissement historique documenté par un
            # échantillon trop petit du backtest courant.
            if n < 8:
                continue
            pf = stats["profit_factor"]
            expectancy = stats["expectancy_r"]
            numeric_pf = pf if isinstance(pf, (int, float)) else (float("inf") if pf == "inf" else None)
            if (n >= HISTORY_MIN_TRADES and numeric_pf is not None
                    and numeric_pf >= HISTORY_MIN_PF
                    and expectancy >= HISTORY_MIN_EXPECTANCY):
                status = "validated"
            elif n >= 8 and ((numeric_pf is not None and numeric_pf < 1.0) or expectancy < 0):
                status = "weak"
            elif n >= 8 and expectancy > 0 and numeric_pf is not None and numeric_pf > 1.0:
                status = "promising"
            else:
                status = "insufficient"
            per_side[side] = {"status": status, "n": n, "pf": pf,
                              "expectancy": expectancy,
                              "note": "backtest CRT clôture RR>=2 actualisé"}
        refreshed[sym] = per_side
    MARKET_HISTORY = refreshed


def real(sym):
    return RESOLVED.get(sym, sym)


def label(sym):
    return MT5.get(sym, sym)


def pick_side(b_d1, b_h4):
    """Le H4 décide. Le Daily n'a qu'un droit de veto : il interdit seulement un trade contre lui."""
    if b_h4 is None:
        return None
    if b_d1 is not None and b_d1 != b_h4:
        return None
    return b_h4


def side_allowed(only, side):
    """Applique la restriction directionnelle identique au scanner et au backtest."""
    return not only or ((only == "buy") == (side == "up"))


def fmt(x):
    return ("%.2f" % x) if abs(x) >= 100 else ("%.5f" % x)


def split_closed(cs, gran, now):
    closed = [c for c in cs if c["t"] + gran <= now]
    forming = [c for c in cs if c["t"] + gran > now]
    return closed, (forming[-1] if forming else None)


def mirror(cs):
    return [{"t": c["t"], "o": -c["o"], "h": -c["l"], "l": -c["h"], "c": -c["c"]} for c in cs]


def mirror_one(c):
    return None if c is None else mirror([c])[0]


def swings(cs, n):
    highs, lows = [], []
    for i in range(n, len(cs) - n):
        h, l = cs[i]["h"], cs[i]["l"]
        if all(h > cs[i - k]["h"] for k in range(1, n + 1)) and all(h >= cs[i + k]["h"] for k in range(1, n + 1)):
            highs.append((i, h))
        if all(l < cs[i - k]["l"] for k in range(1, n + 1)) and all(l <= cs[i + k]["l"] for k in range(1, n + 1)):
            lows.append((i, l))
    return highs, lows


def structure_bias(cs, n):
    hs, ls = swings(cs, n)
    if len(hs) < 2 or len(ls) < 2:
        return None
    if hs[-1][1] > hs[-2][1] and ls[-1][1] > ls[-2][1]:
        return "up"
    if hs[-1][1] < hs[-2][1] and ls[-1][1] < ls[-2][1]:
        return "down"
    return None


def dealing_range_up(cs, n):
    """Dernier grand mouvement haussier : du dernier creux au dernier sommet confirmés."""
    hs, ls = swings(cs, n)
    if not hs:
        return None
    hi_i, hi = hs[-1]
    before = [v for i, v in ls if i < hi_i]
    if not before:
        return None
    lo = before[-1]
    if hi <= lo:
        return None
    return lo, hi


def _dealing_range_snapshot(candles, price, n=2):
    """Dernier swing high au-dessus et swing low sous le prix, tous deux confirmés."""
    hs, ls = swings(candles, n)
    highs = [v for _, v in hs if v > price]
    lows = [v for _, v in ls if v < price]
    if not highs or not lows:
        return None
    lo, hi = lows[-1], highs[-1]
    return (lo, hi) if lo < price < hi else None


def pick_tp(entry, h4c, c1_high, d1c=None, setup_price=None):
    """Cible H4 par parcours des pools, sans priorité fixe de famille.
    Si aucun dealing range H4 ne peut encadrer le prix au setup, CRT opposé seul.
    """
    setup_price = entry if setup_price is None else setup_price
    dr = _dealing_range_snapshot(h4c[-300:], setup_price, SWING_N)
    if dr is None:
        return (c1_high, "haut du CRT (fallback sans dealing range H4)") if c1_high > entry else (None, None)
    hs, _ = swings(h4c[-300:], SWING_N)
    pools = [(h, "swing H4") for _, h in hs if h > entry]
    if h4c:
        prev_h4 = h4c[-1]["h"]
        if prev_h4 > entry:
            pools.append((prev_h4, "previous H4"))
    if d1c:
        pdh = d1c[-1]["h"]
        if pdh > entry:
            pools.append((pdh, "PDH"))
    if c1_high > entry:
        pools.append((c1_high, "haut du CRT"))
    if not pools:
        return None, None
    # Déduplication des pools proches; les familles fusionnées sont journalisées.
    pools.sort(key=lambda x: x[0])
    merged = []
    for price, label_ in pools:
        near = next((g for g in merged if abs(g[0]-price) <= max(abs(price)*EQ_TOL, 1e-12)), None)
        if near:
            near[1].add(label_)
        else:
            merged.append([price, {label_}])
    target = merged[0]
    return target[0], "+".join(sorted(target[1]))


def _atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for k in range(-period, 0):
        c, p = candles[k], candles[k - 1]
        trs.append(max(c["h"] - c["l"], abs(c["h"] - p["c"]), abs(c["l"] - p["c"])))
    return sum(trs) / len(trs) if trs else None


def _v6_buffer(candles):
    if V6_BUFFER_MODE == "fixed":
        return max(0.0, V6_FIXED_BUFFER)
    a = _atr(candles, max(2, V6_ATR_PERIOD))
    return max(0.0, (a or 0.0) * max(0.0, V6_ATR_MULT))


def find_entry(c1, c2, m15c, allm, h4c, mode="fvg", min_rr=None, d1c=None):
    """V6: premier FVG post-réintégration verrouillé; retest strict et SL derrière FVG."""
    min_rr = MIN_RR if min_rr is None else min_rr
    base = {"c1_t": c1["t"], "c1_high": c1["h"], "c1_low": c1["l"]}
    win = [(i, m) for i, m in enumerate(allm) if m["t"] >= c2["t"]]
    sweep = next(((i, m) for i, m in win if m["l"] < c1["l"]), None)
    if sweep is None:
        return dict(base, code="no_sweep")
    s, sweep_candle = sweep
    j = next((i for i in range(s + 1, len(m15c)) if m15c[i]["c"] > c1["l"]), None)
    if j is None:
        return dict(base, code="no_reentry")
    sweep_low = min(m["l"] for _, m in win if m["t"] <= m15c[j]["t"])

    if mode == "close":
        # V6 n'autorise plus l'ancien mode close: le FVG confirmé est obligatoire.
        return dict(base, code="no_fvg", sweep_low=sweep_low)

    # Premier FVG haussier formé après la clôture de réintégration.
    # Le FVG n'est exploitable qu'après clôture de sa troisième bougie.
    locked = None
    for k in range(j + 1, len(m15c) - 2):
        a, third = m15c[k], m15c[k + 2]
        if third["l"] > a["h"]:
            locked = (k, a["h"], third["l"], k + 2)
            break
    if locked is None:
        return dict(base, code="no_fvg", sweep_low=sweep_low)
    k, bottom, top, confirmed = locked

    # L'invalidation du FVG initial annule définitivement ce setup H4.
    # On ne remplace jamais le FVG verrouillé par un FVG ultérieur.
    for q in range(confirmed + 1, len(m15c)):
        candle = m15c[q]
        if candle["c"] < bottom:
            return dict(base, code="fvg_invalidated", fvg_bottom=bottom, fvg_top=top,
                        fvg_t=m15c[k]["t"], sweep_low=sweep_low)

    last = m15c[-1]
    # Le retest doit réellement pénétrer la zone et clôturer strictement au-dessus
    # du bord proche (top pour un FVG haussier). Une égalité ne valide pas.
    retest_found = None
    for q in range(confirmed + 1, len(m15c)):
        c = m15c[q]
        intersects = c["l"] <= top and c["h"] >= bottom
        penetrates = c["l"] < top
        if intersects and penetrates and c["c"] > top:
            retest_found = (q, c)
    if retest_found is None or retest_found[0] != len(m15c) - 1:
        return dict(base, code="waiting_retrace", fvg_bottom=bottom, fvg_top=top,
                    fvg_t=m15c[k]["t"], sweep_low=sweep_low)

    entry = last["c"]
    buffer = _v6_buffer(m15c[max(0, len(m15c)-V6_ATR_PERIOD-2):])
    sl = bottom - buffer
    if entry <= sl:
        return dict(base, code="rr_low", fvg_bottom=bottom, fvg_top=top, sweep_low=sweep_low)
    setup_h4 = [x for x in h4c if x["t"] <= c2["t"]]
    setup_d1 = [x for x in (d1c or []) if x["t"] + 86400 <= c2["t"] + 14400]
    tp, label = pick_tp(entry, setup_h4, c1["h"], setup_d1, setup_price=c2["c"])
    h4_dr = _dealing_range_snapshot(setup_h4[-300:], c2["c"], SWING_N)
    if tp is None or tp <= entry:
        return dict(base, code="rr_low", fvg_bottom=bottom, fvg_top=top, sweep_low=sweep_low)
    risk = entry - sl
    rr = (tp - entry) / risk if risk > 0 else 0.0
    out = dict(base, fvg_bottom=bottom, fvg_top=top, fvg_t=m15c[k]["t"],
               sweep_low=sweep_low, sweep_t=sweep_candle["t"], reentry_t=m15c[j]["t"],
               reentry_o=m15c[j]["o"], reentry_h=m15c[j]["h"], reentry_l=m15c[j]["l"],
               reentry_c=m15c[j]["c"], retest_t=last["t"], retest_close=last["c"],
               entry=entry, sl=sl, tp=tp, tp_label=label, rr=rr,
               fvg_buffer=buffer,
               h4_dr_low=(h4_dr[0] if h4_dr else None), h4_dr_high=(h4_dr[1] if h4_dr else None),
               code="signal" if rr >= min_rr else "rr_low")
    return out


def evaluate_buy(d1c, h4c, h4f, m15c, m15f, mode="fvg", min_rr=None, d1_ref=None):
    # Les décisions de stratégie (zone, sweep, FVG et retest) utilisent
    # exclusivement des M15 clôturées, comme dans le backtest.
    allm = m15c
    price = m15c[-1]["c"]
    rng = dealing_range_up(d1c, SWING_N)
    if not rng:
        return {"code": "no_range"}
    lo, hi = rng
    eq = (lo + hi) / 2.0
    base = {"price": price, "range_lo": lo, "range_hi": hi, "eq": eq}
    if not (lo <= price < eq):
        return dict(base, code="not_in_zone")

    pairs = []
    if h4f is not None and h4c:
        pairs.append((h4c[-1], h4f, False))   # la bougie 2 est en cours
    if len(h4c) >= 2:
        pairs.append((h4c[-2], h4c[-1], True))  # la bougie 2 vient de se fermer
    best = dict(base, code="no_crt")
    for c1, c2, closed in pairs:
        if not (c2["l"] < c1["l"]) or c2["h"] > c1["h"]:
            continue  # pas de sweep du bas, ou les deux côtés balayés
        if closed and not (c1["l"] < c2["c"] <= c1["h"]):
            continue  # la bougie 2 n'a pas clôturé dans le range
        res = dict(base, **find_entry(c1, c2, m15c, allm, h4c, mode, min_rr, d1_ref))
        if RANK[res["code"]] > RANK[best["code"]]:
            best = res
    return best


def unmirror(m):
    out = dict(m)
    if "price" in m:
        out["price"] = -m["price"]
    if "eq" in m:
        out["eq"] = -m["eq"]
    if "range_lo" in m:
        out["range_lo"], out["range_hi"] = -m["range_hi"], -m["range_lo"]
    if "fvg_bottom" in m:
        out["fvg_bottom"], out["fvg_top"] = -m["fvg_top"], -m["fvg_bottom"]
    if "c1_high" in m:
        out["c1_low"], out["c1_high"] = -m["c1_high"], -m["c1_low"]
    for k in ("entry", "sl", "tp", "sweep_low"):
        if k in m:
            out[k] = -m[k]
    if all(k in m for k in ("reentry_o", "reentry_h", "reentry_l", "reentry_c")):
        out["reentry_o"] = -m["reentry_o"]
        out["reentry_h"] = -m["reentry_l"]
        out["reentry_l"] = -m["reentry_h"]
        out["reentry_c"] = -m["reentry_c"]
    labels = {"Equal Highs": "Equal Lows", "haut du CRT": "bas du CRT", "sommet H4": "creux H4"}
    if "tp_label" in m:
        out["tp_label"] = labels.get(m["tp_label"], m["tp_label"])
    return out


def analyze(sym, d1, h4, m15, now):
    """Version en direct : sépare les bougies fermées / en cours, puis analyse."""
    if m15 and now - (m15[-1]["t"] + 900) > 1800:
        return {"resume": "marché fermé ou données anciennes"}, None
    d1c, _ = split_closed(d1, 86400, now)
    h4c, h4f = split_closed(h4, 14400, now)
    m15c, m15f = split_closed(m15, 900, now)
    info, sig = analyze_core(d1c, h4c, h4f, m15c, m15f, None, SIDE_ONLY.get(sym))
    if sig:
        grade, reason = classify_setup(sym, sig)
        sig["grade"] = grade
        sig["grade_reason"] = reason
        info["grade"] = grade
        info["grade_reason"] = reason
        info["resume"] = info["resume"].replace("SETUP À CLASSER", "SETUP " + grade)
    return info, sig


def analyze_core(d1c, h4c, h4f, m15c, m15f, biases=None, only=None, mode=None, min_rr=None):
    """Retourne (infos_pour_la_page, signal_ou_None)."""
    mode = mode or ENTRY_MODE
    if len(d1c) < 15 or len(h4c) < 15 or len(m15c) < 15:
        return {"resume": "pas assez de bougies reçues (D1 %d, H4 %d, M15 %d)" % (len(d1c), len(h4c), len(m15c))}, None

    if biases:
        b_d1, b_h4 = biases
    else:
        b_d1, b_h4 = structure_bias(d1c, SWING_N), structure_bias(h4c, SWING_N)
    head = "D1 %s · H4 %s" % (ARROW[b_d1], ARROW[b_h4])
    px = " · prix " + fmt((m15f or m15c[-1])["c"])
    side = pick_side(b_d1, b_h4)
    if side is None:
        why = "H4 sans structure claire" if b_h4 is None else "Daily contre le H4"
        return {"resume": head + " · " + why + ", pas de trade" + px}, None

    if not side_allowed(only, side):
        return {"resume": head + " · ici on ne trade que les %s (spikes), mais le H4 est %s : pas de trade%s" % (
            "achats" if only == "buy" else "ventes", "haussier" if side == "up" else "baissier", px)}, None

    buy = (side == "up")
    zone = "discount" if buy else "premium"
    rng_src = d1c if PD_RANGE == "D1" else h4c
    if buy:
        res = evaluate_buy(rng_src, h4c, h4f, m15c, m15f, mode, min_rr, d1c)
    else:
        res = unmirror(evaluate_buy(mirror(rng_src), mirror(h4c), mirror_one(h4f), mirror(m15c), mirror_one(m15f), mode, min_rr, mirror(d1c)))

    code = res["code"]
    if RANK[code] >= RANK["no_crt"]:
        head += " · en " + zone
    msgs = {
        "no_range": "range %s introuvable" % PD_RANGE,
        "not_in_zone": "prix pas en %s (prix %s, équilibre %s)" % (zone, fmt(res.get("price", 0)), fmt(res.get("eq", 0))),
        "no_crt": "pas de CRT H4 en cours",
        "no_sweep": "CRT H4 vu, sweep pas encore visible en M15",
        "no_reentry": "sweep fait, pas encore de clôture M15 dans le range",
        "fvg_invalidated": "premier FVG M15 invalidé : setup H4 annulé, aucun remplacement",
        "no_fvg": ("retour dans le range, pas de FVG M15 valide" if mode == "fvg" else "retour dans le range déjà passé (l'entrée se fait à la clôture de cette bougie)"),
        "waiting_retrace": "FVG M15 repéré, attente d'une bougie de retest clôturée",
        "rr_low": "setup trouvé mais gain/risque trop faible (%s)" % (("%.1f" % res["rr"]) if "rr" in res else "pas de cible"),
        "signal": "SETUP À CLASSER",
    }
    info = {"resume": head + " · " + msgs[code] + px, "code": code,
            "c1_t": res.get("c1_t"), "fvg_t": res.get("fvg_t"), "rr": res.get("rr")}
    if code == "signal":
        sig = dict(res)
        sig["side"] = "buy" if buy else "sell"
        return info, sig
    return info, None


def build_text(sym, sig):
    buy = sig["side"] == "buy"
    return "\n".join([
        "%s %s - Setup %s" % ("ACHAT" if buy else "VENTE", label(sym), sig.get("grade", "B")),
        "Classement : %s" % sig.get("grade_reason", "historique directionnel non validé"),
        "Biais H4 %s (Daily pas contre), prix en %s" % ("haussier" if buy else "baissier", "discount" if buy else "premium"),
        "CRT H4 : sweep puis retour dans le range",
        ("Entrée au marché, à la clôture M15 de réintégration" if sig["fvg_bottom"] == sig["fvg_top"]
         else "FVG M15 retesté : %s - %s" % (fmt(sig["fvg_bottom"]), fmt(sig["fvg_top"]))),
        ("Entrée au marché à la clôture de la bougie M15 de retest : %s" % fmt(sig["entry"])
         if sig["fvg_bottom"] != sig["fvg_top"] else "Entrée repère : %s" % fmt(sig["entry"])),
        "Stop : %s (derrière le FVG initial + buffer, distance %s, soit %.2f%% du prix)" % (fmt(sig["sl"]), fmt(abs(sig["entry"] - sig["sl"])), abs(sig["entry"] - sig["sl"]) / abs(sig["entry"]) * 100),
        "TP : %s (%s, soit %.2f%% du prix)" % (fmt(sig["tp"]), sig["tp_label"], abs(sig["tp"] - sig["entry"]) / abs(sig["entry"]) * 100),
        "Gain/risque : %.1f pour 1" % sig["rr"],
        "Prix Deriv au signal : %s. Compare avec ton MT5 avant d'entrer." % fmt(sig["price"]),
        "Aucun ordre n'est passé : vérifie sur le graphique.",
    ])


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


def _d1_runner_target(entry, d1c, buy, partial_price=None, frozen_range=None):
    """Prochaine liquidité D1 non consommée: swings, EQH/EQL, PDH/PDL.
    Les niveaux sont triés par prix (parcours IRL/ERL), sans priorité fixe de famille.
    """
    if frozen_range is False:
        return None, None, False
    if len(d1c) < 5:
        return None, None, frozen_range
    dr = frozen_range if frozen_range is not None else _dealing_range_snapshot(
        d1c, entry if partial_price is None else partial_price, SWING_N)
    if dr is None:
        return None, None, None
    hs, ls = swings(d1c, SWING_N)
    raw = [(v, "swing D1") for _, v in (hs if buy else ls)
           if (v > entry if buy else v < entry)]
    if len(d1c) >= 2:
        prev = d1c[-1]["h"] if buy else d1c[-1]["l"]
        if prev > entry if buy else prev < entry:
            raw.append((prev, "PDH" if buy else "PDL"))
    # Equal highs/lows: groupe de deux swings ou plus dans la tolérance configurée.
    pivots = [v for _, v in (hs if buy else ls)]
    for n, v in enumerate(pivots):
        cluster = [x for x in pivots if abs(x-v) <= abs(v)*EQ_TOL]
        if len(cluster) >= 2 and (v > entry if buy else v < entry):
            raw.append((sum(cluster)/len(cluster), "EQH" if buy else "EQL"))
    if not raw:
        return None, None, dr
    # Déduplication des pools proches; on garde toutes les étiquettes associées.
    raw.sort(key=lambda x: x[0], reverse=not buy)
    groups=[]
    for price, label_ in raw:
        found=next((g for g in groups if abs(g[0]-price) <= max(abs(price)*EQ_TOL, 1e-12)), None)
        if found:
            found[1].add(label_)
        else:
            groups.append([price, {label_}])
    chosen=groups[0]
    return chosen[0], "+".join(sorted(chosen[1])), dr


def _new_fvg_trail(candles, j, buy, current_sl, buffer):
    """Renvoie un stop resserré seulement après nouveau FVG confirmé + retest strict."""
    if j < 3:
        return current_sl
    a, third, ret = candles[j-3], candles[j-1], candles[j]
    if buy and third["l"] > a["h"]:
        bottom, top = a["h"], third["l"]
        if ret["l"] < top and ret["h"] >= bottom and ret["c"] > top:
            candidate = bottom - buffer
            return max(current_sl, candidate)
    if not buy and third["h"] < a["l"]:
        bottom, top = third["h"], a["l"]
        if ret["h"] > bottom and ret["l"] <= top and ret["c"] < bottom:
            candidate = top + buffer
            return min(current_sl, candidate)
    return current_sl


def simulate_v6(sig, m15, i, d1_history, max_bars=288):
    """Sortie V6: 50% sur liquidité H4, 50% sur D1 avec trailing FVG.
    Si TP et stop sont touchés sur la même bougie sans ordre connu, stop prioritaire.
    """
    buy = sig["side"] == "buy"
    entry, initial_sl, h4_tp = sig["entry"], sig["sl"], sig["tp"]
    risk = abs(entry - initial_sl)
    if not m15 or risk <= 0 or i < 0 or i >= len(m15):
        return "invalid", 0.0, i
    d1_tp = None
    d1_label = None
    d1_frozen_range = None
    sl_runner = initial_sl
    if V6_RUNNER_PROTECTION == "breakeven":
        sl_runner = entry
    partial_done = False
    realized = 0.0
    last_j = min(len(m15)-1, i+max_bars)
    buffer = max(0.0, float(sig.get("fvg_buffer", 0.0)))
    for j in range(i+1, last_j+1):
        c = m15[j]
        sl_now = initial_sl if not partial_done else sl_runner
        hit_sl = c["l"] <= sl_now if buy else c["h"] >= sl_now
        target = h4_tp if not partial_done else d1_tp
        hit_target = (c["h"] >= target if buy else c["l"] <= target) if target is not None else False
        if hit_sl:
            r_stop = ((sl_now-entry)/risk if buy else (entry-sl_now)/risk)
            if not partial_done:
                return "loss", round(r_stop, 5), j
            return "runner_stop", round(realized + 0.5*r_stop, 5), j
        if not partial_done and hit_target:
            r_h4 = ((h4_tp-entry)/risk if buy else (entry-h4_tp)/risk)
            realized = 0.5*r_h4
            partial_done = True
            # Snapshot D1 à l'instant exact de la prise partielle H4.
            partial_time = m15[j]["t"] + 900
            d1_closed = [x for x in d1_history if x["t"] + 86400 <= partial_time]
            d1_tp, d1_label, d1_frozen_range = _d1_runner_target(entry, d1_closed, buy, c["c"])
            if d1_frozen_range is None:
                d1_frozen_range = False  # absence de DR gelée : pas de cible D1 inventée ensuite
            if len(d1_closed) >= 5 and d1_frozen_range is not False:
                dh, dl = swings(d1_closed, SWING_N)
                above = [v for _, v in dh if v > c["c"]]
                below = [v for _, v in dl if v < c["c"]]
                if above and below:
                    d1_frozen_range = {"low": below[-1], "high": above[-1], "time": partial_time}
            if d1_tp is None and V6_RUNNER_PROTECTION == "breakeven":
                sl_runner = entry
            continue
        if partial_done:
            sl_runner = _new_fvg_trail(m15, j, buy, sl_runner, buffer)
            # Une nouvelle cible D1 peut apparaître après l'activation du trailing.
            if d1_tp is None:
                now = m15[j]["t"] + 900
                d1_closed = [x for x in d1_history if x["t"] + 86400 <= now]
                d1_tp, d1_label, _ = _d1_runner_target(entry, d1_closed, buy, c["c"], d1_frozen_range)
            if d1_tp is not None and (c["h"] >= d1_tp if buy else c["l"] <= d1_tp):
                r_d1 = ((d1_tp-entry)/risk if buy else (entry-d1_tp)/risk)
                return "h4_d1", round(realized + 0.5*r_d1, 5), j
    close = m15[last_j]["c"]
    if partial_done:
        r_runner = ((close-entry)/risk if buy else (entry-close)/risk)
        return ("open" if i+max_bars >= len(m15) else "timeout"), round(realized+0.5*r_runner,5), last_j
    r = ((close-entry) if buy else (entry-close))/risk
    return ("open" if i+max_bars >= len(m15) else "timeout"), r, last_j


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
            cache[key] = (structure_bias(d1[max(0, kd - 150):kd], SWING_N),
                          structure_bias(h4[max(0, kh - 300):kh], SWING_N))
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
        info, sig = analyze_core(d1c, h4c, h4f, m15c, None, (b_d1, b_h4), only, mode, lo_rr)
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
                    outcome = simulate_v6(sig, m15, i, d1)
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
                    "h4_dr_low": sig.get("h4_dr_low"), "h4_dr_high": sig.get("h4_dr_high"),
                    "fvg_buffer": sig.get("fvg_buffer"),
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
    w = sum(1 for t in trades if t["res"] == "win" or t["res"] == "h4_d1" or (t["res"] == "runner_stop" and float(t.get("r", 0)) > 0))
    l = sum(1 for t in trades if t["res"] == "loss" or (t["res"] == "runner_stop" and float(t.get("r", 0)) < 0))
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
            "gagnes": w, "perdus": l, "expires": to, "ouverts": op, "r_total": round(total_r, 2),
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
    # Comptage hebdomadaire explicite, semaines à zéro incluses.
    week_counts = []
    cursor = window_start
    while cursor < window_end:
        nxt = min(window_end, cursor + 7 * 86400)
        count = sum(1 for t in trades if cursor <= t["t"] < nxt)
        week_counts.append({"debut_utc": int(cursor), "fin_utc": int(nxt), "trades": count})
        cursor = nxt
    active_weeks = sum(1 for w in week_counts if w["trades"] > 0)
    weekly_activity = {"semaines_total": len(week_counts), "semaines_avec_trade": active_weeks,
                       "semaines_sans_trade": len(week_counts)-active_weeks,
                       "part_semaines_actives_pct": round(100*active_weeks/len(week_counts), 1) if week_counts else 0,
                       "moyenne_trades_semaine": round(len(trades)/len(week_counts), 2) if week_counts else 0,
                       "detail": week_counts}
    return {"setups": len(trades), "activite_hebdomadaire": weekly_activity,
            "gagnes": sum(1 for t in trades if t["res"] == "win" or (t["res"] == "runner_stop" and float(t.get("r", 0)) > 0) or t["res"] == "h4_d1"),
            "perdus": sum(1 for t in trades if t["res"] == "loss" or (t["res"] == "runner_stop" and float(t.get("r", 0)) < 0)),
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
BT_THRS = (1.0, 1.5, 2.0)


def trade_audit_groups(trades, key_fields):
    """Regroupe les trades pour rechercher des causes récurrentes sans masquer les cas individuels."""
    buckets = {}
    for trade in trades:
        key = tuple(trade.get(k) for k in key_fields)
        buckets.setdefault(key, []).append(trade)
    out = []
    for key, group in sorted(buckets.items(), key=lambda item: tuple(str(x) for x in item[0])):
        closed = [t for t in group if t.get("res") != "open"]
        wins = sum(t.get("res") == "win" or t.get("res") == "h4_d1" or (t.get("res") == "runner_stop" and float(t.get("r", 0)) > 0) for t in group)
        losses = sum(t.get("res") == "loss" or (t.get("res") == "runner_stop" and float(t.get("r", 0)) < 0) for t in group)
        total_r = round(sum(float(t.get("r", 0) or 0) for t in closed), 3)
        gross_win = sum(float(t.get("r", 0) or 0) for t in closed if float(t.get("r", 0) or 0) > 0)
        gross_loss = -sum(float(t.get("r", 0) or 0) for t in closed if float(t.get("r", 0) or 0) < 0)
        out.append({**dict(zip(key_fields, key)), "trades": len(group), "gagnes": wins,
                    "perdus": losses, "ouverts": sum(t.get("res") == "open" for t in group),
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
                outcome_sum = aggregate["gagnes"] + aggregate["perdus"] + aggregate["expires"] + aggregate["ouverts"]
                combos.append({"mode": mode, "min_rr": thr, "total": aggregate,
                               "validation_chronologique": _chronological_validation(combo_trades, window_start, window_end),
                               "sensibilite_couts_hors_echantillon": _cost_sensitivity(combo_trades, window_start, window_end, default_cost_r, cost_by_symbol),
                               "reconciliation": {"trades_internes": len(combo_trades),
                                                  "setups_lignes_marches": row_setups,
                                                  "somme_resultats": outcome_sum,
                                                  "ecart_trades_vs_lignes": len(combo_trades) - row_setups,
                                                  "ecart_resultats_vs_setups": len(combo_trades) - outcome_sum},
                               "lignes": combo_rows})
        primary_audit = alltr[("fvg", 2.0)]
        enrich_trade_quality(primary_audit)
        losses_audit = [t for t in primary_audit if float(t.get("r", 0)) < 0]
        BT["results"] = {"version": "V6", "strategie_reference": "premier FVG M15 verrouillé, retest strict, SL derrière FVG, sortie 50/50 H4/D1 + trailing FVG", "protection_runner": V6_RUNNER_PROTECTION, "buffer_mode": V6_BUFFER_MODE, "buffer_fixed": V6_FIXED_BUFFER, "atr_period": V6_ATR_PERIOD, "atr_multiplier": V6_ATR_MULT,
                         "jours": BT_DAYS, "jours_new": BT_DAYS // 3,
                         "fenetre_debut_utc": window_start, "fenetre_fin_utc": window_end,
                         "bougies_m15_incompletes_exclues": True, "borne_commune_toutes_unites": True,
                         "marches": n_ok, "couverture_historique": coverage,
                         "audit_trades_v6_fvg_rr2": primary_audit,
                         "audit_pertes_v6_fvg_rr2": losses_audit,
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
