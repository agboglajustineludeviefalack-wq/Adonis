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

