import os
import re
import json
import time
import html
import base64
import asyncio
import hashlib
import gzip
import secrets
import uuid
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
    "1HZ50V,BOOM500,CRASH500,CRASH1000,BOOM1000,JD10,stpRNG"
)
_cfg = [x.strip() for x in os.environ.get("SCAN_SYMBOLS", _DEFAULT_SYMBOLS).split(",") if x.strip()]
# frxBROUSD est exclu même s’il est encore présent dans SCAN_SYMBOLS sur Railway.
_cfg = [x for x in _cfg if x.split("|")[0].strip() != "frxBROUSD"]
SYMBOLS = [x.split("|")[0].strip() for x in _cfg]
HINTS = {x.split("|")[0].strip(): x.split("|")[1].strip() for x in _cfg if "|" in x}
# Noms tels qu'ils apparaissent dans ton MT5 (pour les alertes et la page)
MT5 = {
    "frxXAUUSD": "XAUUSD", "cryBTCUSD": "BTCUSD", "frxEURUSD": "EURUSD", "frxUSDJPY": "USDJPY",
    "frxAUDCAD": "AUDCAD", "1HZ50V": "Volatility 50 (1s)", "BOOM500": "Boom 500", "CRASH500": "Crash 500",
    "CRASH1000": "Crash 1000", "BOOM1000": "Boom 1000", "JD10": "Jump 10", "stpRNG": "Step Index",
}
# Tous les marchés, Boom/Crash compris, suivent le biais structurel H4.
SIDE_ONLY = {}  # Aucune direction forcée par symbole; H4 haussier=BUY, baissier=SELL
RESOLVED = {}   # nom demandé -> vrai code chez Deriv (si on a dû le chercher par mot-clé)
NAMES = {}      # code Deriv -> nom affiché par Deriv
PD_RANGE = "H4"  # Conservé pour compatibilité; aucun filtre Premium/Discount en ATLAS
SWING_N = int(os.environ.get("SWING_N", "2"))        # bougies de chaque côté d'un sommet/creux
MIN_RR = 0.0  # Les seuils RR sont des ventilations statistiques, pas des filtres
EQ_TOL = float(os.environ.get("EQ_TOL", "0.0005"))
OTE_LOW = float(os.environ.get("OTE_LOW", "0.62"))
OTE_HIGH = float(os.environ.get("OTE_HIGH", "0.79"))
SL_ATR_MULT = float(os.environ.get("SL_ATR_MULT", "0.10"))
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
# Diagnostics structurés de pagination Deriv, réinitialisés à chaque backtest.
BT_HISTORY_DIAGNOSTICS = {}


HTML_CONTENT = r"""
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ADONIS ATLAS - Terminal Pro</title>
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
    <a class="btn btn-gray" id="btDownload" href="/api/backtest/download" style="display:none; margin-top:8px;">Télécharger le JSON du backtest</a>
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
  $("btDownload").style.display = (j.done && j.results) ? "inline-block" : "none";
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
    d.textContent = MODE_NAMES[mode] + " — funnel réconcilié (" + res.marches + " marchés, " + res.jours + " jours) : étapes analysées " + f.steps + " · biais H4 directionnel " + f.bias + " · CRT avec sweep (ranges uniques) " + f.crt + " · FVG candidates uniques " + f.fvg + " · en attente de confirmation " + (f.waiting_confirmation || 0) + " · confirmations uniques " + (f.confirmed || 0) + " · confirmations RR ≥ 2 " + (f.rr_ge2 || 0) + " · confirmations RR < 2 " + f.rr + " · zones rejetees (TP/risk invalide) " + (f.rr_uncomputable || 0) + " · passages sans CRT " + (f.no_crt_steps || 0) + " · passages sans FVG " + (f.no_fvg_steps || 0) + (f.zones_etats_final ? " · état final des zones: " + f.zones_etats_final.candidates_uniques + " candidates / " + f.zones_etats_final.en_attente + " en attente / " + f.zones_etats_final.confirmees + " confirmées / " + f.zones_etats_final.rejetees_tp_risque + " rejetées TP-risque" : "") + " · trades portfolio RR ≥ 2 " + (c2 ? c2.total.setups : 0);
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
    sm.textContent = MODE_NAMES[c.mode] + " · scénario de portefeuille filtré RR ≥ " + c.min_rr + " : " + t.setups + " trades · " + t.gagnes + " gagnés · " + t.perdus + " perdus · " + (t.equilibres || 0) + " à l'équilibre · " + (t.expires || 0) + " expirés · " + (t.ouverts || 0) + " encore ouverts · " + sign(t.r_total) + " R (" + sign(per) + " R/trade) · avant les " + res.jours_new + " derniers jours : " + t.n_old + " trades, " + sign(t.r_old) + " R · " + res.jours_new + " derniers jours : " + t.n_new + " trades, " + sign(t.r_new) + " R";
    det.appendChild(sm);
    const audit = document.createElement("div");
    audit.style.marginTop = "6px";
    audit.style.padding = "8px";
    audit.style.background = "rgba(255,255,255,.04)";
    const p = t.periods || [];
    const pf = t.profit_factor === null ? "—" : (t.profit_factor === "inf" ? "∞" : t.profit_factor);
    audit.textContent = "Audit : PF " + pf + " · espérance " + sign(t.expectancy_r) + " R/trade · drawdown réalisé max " + t.max_drawdown_r + " R · positions simultanées max " + (t.max_positions_simultanees || 0) + " · série max pertes " + t.max_pertes_consecutives +
      " · sans top 1/2/3/5 : " + [t.sans_best1_r, t.sans_best2_r, t.sans_best3_r, t.sans_best5_r].map(x => sign(x) + " R").join(" / ") +
      " · périodes : " + p.map((x, i) => "P" + (i + 1) + " " + x.n + " trades / " + sign(x.r) + " R").join(" · ");
    det.appendChild(audit);
    c.lignes.forEach(l => {
      const d = document.createElement("div");
      d.style.marginTop = "4px";
      const bpf = l.achats_pf === null ? "—" : (l.achats_pf === "inf" ? "∞" : l.achats_pf);
      const spf = l.ventes_pf === null ? "—" : (l.ventes_pf === "inf" ? "∞" : l.ventes_pf);
      d.textContent = l.erreur ? (l.symbole + " — erreur : " + l.erreur) : (l.symbole + " — " + l.setups + " setups · " + l.gagnes + " G · " + l.perdus + " P · " + (l.equilibres || 0) + " BE · " + (l.expires || 0) + " expirés · " + (l.ouverts || 0) + " ouverts · " + sign(l.r_total) + " R · PF " + (l.profit_factor === null ? "—" : (l.profit_factor === "inf" ? "∞" : l.profit_factor)) + " · DD réalisé " + l.max_drawdown_r + " R · positions simultanées max " + (l.max_positions_simultanees || 0) + " · ACHATS " + l.achats + " trades / " + sign(l.r_achats) + " R / PF " + bpf + " / esp. " + sign(l.achats_expectancy_r) + " R · VENTES " + l.ventes + " trades / " + sign(l.r_ventes) + " R / PF " + spf + " / esp. " + sign(l.ventes_expectancy_r) + " R");
      det.appendChild(d);
    });
    list.appendChild(det);
  });
  const note = document.createElement("div");
  note.className = "small";
  note.style.marginTop = "6px";
  note.textContent = "ADONIS ATLAS : biais de tendance H4; CRT AMD conservé jusqu’à invalidation par clôture H4 au-delà de 0,10 ATR; FVG M15 le plus récent prioritaire; entrée sur clôture de reclaim après test; SL sur mèche du test M15 + buffer ATR; TP liquidité H4; trailing après clôture M15 au-delà de la borne CRT, puis nouveau FVG, retest et confirmation. Aucun filtre Premium/Discount ni taille moyenne H4. Attention : les lignes RR ≥ X sont des simulations de portefeuilles distinctes avec filtre RR, et non de simples statistiques post-hoc. Le rapport fournit aussi une ventilation RR descriptive sur les trades de référence.";
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
#  ANALYSE ADONIS ATLAS (biais H4 + CRT persistant + FVG M15)
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

def _structure_bias_timeline(cs, n=SWING_N):
    """Biais structurel connu à chaque clôture H4, sans confirmer un swing avant ses n bougies droites."""
    highs=[]; lows=[]; timeline=[None]*len(cs)
    for end in range(len(cs)):
        pivot=end-n
        if pivot >= n:
            h,l=cs[pivot]["h"],cs[pivot]["l"]
            if all(h>cs[pivot-k]["h"] for k in range(1,n+1)) and all(h>=cs[pivot+k]["h"] for k in range(1,n+1)):
                highs.append((pivot,h))
            if all(l<cs[pivot-k]["l"] for k in range(1,n+1)) and all(l<=cs[pivot+k]["l"] for k in range(1,n+1)):
                lows.append((pivot,l))
        if len(highs)>=2 and len(lows)>=2:
            if highs[-1][1]>highs[-2][1] and lows[-1][1]>lows[-2][1]:
                timeline[end]="up"
            elif highs[-1][1]<highs[-2][1] and lows[-1][1]<lows[-2][1]:
                timeline[end]="down"
    return timeline

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
    """Choisit la liquidité H4 la plus proche parmi les niveaux connus à T.

    Un swing n'est admissible qu'après ses SWING_N bougies de confirmation droite.
    h4c est le préfixe H4 clôturé disponible à l'instant du signal M15.
    """
    if not h4c:
        return None,"aucune liquidité H4 identifiée",None
    decision_t = int(h4c[-1]["t"]) + 14400
    offset=max(0,len(h4c)-250)
    hs,ls=swings(h4c[-250:],SWING_N)
    candidates=[]
    for idx,value in (hs if side=="buy" else ls):
        abs_idx = offset + idx
        confirmed_idx = abs_idx + SWING_N
        if confirmed_idx >= len(h4c):
            continue
        available_at = int(h4c[confirmed_idx]["t"]) + 14400
        if available_at > decision_t:
            continue
        if value>entry if side=="buy" else value<entry:
            candidates.append({"price":value,"type":"swing_h4","index":abs_idx,
                              "time":int(h4c[abs_idx]["t"]),
                              "confirmed_at":available_at})
    edge=crt["h"] if side=="buy" else crt["l"]
    if (edge>entry if side=="buy" else edge<entry):
        candidates.append({"price":edge,"type":"crt_edge","index":None,
                           "time":crt.get("range_t"),
                           "confirmed_at":int(crt.get("range_t") or 0)+14400})
    # Défense finale : aucun niveau dont la confirmation serait postérieure
    # à la clôture H4 connue au moment où le signal M15 est évalué.
    candidates=[x for x in candidates if int(x.get("confirmed_at") or 0) <= decision_t]
    if not candidates: return None,"aucune liquidité H4 identifiée",None
    chosen=min(candidates,key=lambda x:x["price"]) if side=="buy" else max(candidates,key=lambda x:x["price"])
    chosen["decision_h4_close"] = decision_t
    return chosen["price"],"liquidité H4 (%s)"%chosen["type"],chosen

def classify_setup(sym,sig):
    rr=float(sig.get("rr",0) or 0)
    return ("A+" if rr>=3.5 else "A" if rr>=2 else "B", "classement descriptif uniquement; aucun filtre historique")

def refresh_market_history(alltr): return None

def _active_crt(side, h4c):
    """CRT H4 à deux bougies : range, puis sweep avec clôture de réintégration.

    Dès que la bougie de sweep est clôturée dans le range, le CRT est actif.
    Aucune troisième bougie H4 d'expansion n'est exigée : la recherche M15
    peut commencer immédiatement après la clôture de la bougie de sweep.
    """
    if len(h4c) < 2:
        return None
    candidates = []
    bias_timeline = _structure_bias_timeline(h4c, SWING_N)
    for k in range(1, len(h4c)):
        first, manip = h4c[k-1], h4c[k]
        if side == "buy":
            sweep_valid = (manip["l"] < first["l"] and
                           manip["c"] > first["l"] and manip["c"] <= first["h"])
        else:
            sweep_valid = (manip["h"] > first["h"] and
                           manip["c"] < first["h"] and manip["c"] >= first["l"])
        if sweep_valid:
            candidates.append({
                "h": first["h"], "l": first["l"], "range_t": first["t"],
                "sweep_t": manip["t"], "sweep_o": manip["o"],
                "sweep_high": manip["h"], "sweep_low": manip["l"],
                "sweep_c": manip["c"],
                # Compatibilité des champs historiques : aucune expansion H4 requise.
                "expansion_t": None, "expansion_o": None,
                "expansion_high": None, "expansion_low": None,
                "expansion_c": None,
            })
    # Un CRT plus récent invalidé ne masque pas un CRT antérieur encore valide.
    for found in reversed(candidates):
        idx = next((i for i, c in enumerate(h4c) if c["t"] == found["sweep_t"]), None)
        if idx is None:
            continue
        invalidated = False
        for j in range(idx + 1, len(h4c)):
            # Un changement de structure H4 confirmé dans le sens opposé
            # rend ce CRT obsolète, même si sa borne de sweep n'a pas été cassée.
            opposite_bias = "down" if side == "buy" else "up"
            if bias_timeline[j] == opposite_bias:
                invalidated = True
                break
            c = h4c[j]
            # ATR calculé uniquement avec les bougies clôturées AVANT la
            # bougie testée : sa propre amplitude ne doit pas élargir son seuil
            # d'invalidation et rendre une cassure violente artificiellement valide.
            hist = h4c[max(0, j-15):j]
            av = atr(hist, 14) or abs(c["c"]) * 0.0005
            if side == "buy" and c["c"] < found["l"] - 0.10 * av:
                invalidated = True
                break
            if side == "sell" and c["c"] > found["h"] + 0.10 * av:
                invalidated = True
                break
        if not invalidated:
            return found
    return None


def _find_side_signal(side,h4c,h4f,m15c,d1c,mode="fvg"):
    """ATLAS: FVG/IFVG lifecycle tracked chronologically; test and reclaim must be separate closed candles."""
    if len(h4c) < 20 or len(m15c) < 30:
        return {"code":"no_range"}
    crt = _active_crt(side, h4c)
    if not crt:
        return {"code":"no_crt"}
    post = [m for m in m15c if m["t"] >= crt["sweep_t"] + 14400]
    if len(post) < 4:
        return {"code":"no_fvg", "c1_t":crt["range_t"],
                "expansion_t":crt["expansion_t"],"expansion_c":crt["expansion_c"]}
    zones = fvg_zones(post, side)
    # Une absence de FVG directe ne doit pas court-circuiter la recherche IFVG.
    last_i = len(post)-1
    last = post[last_i]
    candidate_rejected = False
    rejected_meta = None
    candidate_waiting = False
    candidate_waiting_meta = None
    ifvg_waiting_meta = None
    fvg_signal_candidate = None
    ifvg_signal_candidate = None
    invalidated_zones = []
    confirmed_zones = []
    # Lifecycle for each same-direction FVG: fresh -> tested -> confirmed OR invalidated.
    # A close through the far edge invalidates the original zone. No same-candle test/reclaim.
    for idx, lo, hi, ft in reversed(zones):
        tested = None
        invalidated = False
        rejected_this_zone = False
        confirmed_this_zone = False
        for j in range(idx+1, len(post)):
            bar = post[j]
            if side == "buy" and bar["c"] < lo:
                invalidated = True; break
            if side == "sell" and bar["c"] > hi:
                invalidated = True; break
            touched = bar["l"] <= hi and bar["h"] >= lo
            if tested is None and touched:
                tested = (j, bar)
                continue
            if tested is not None and j > tested[0]:
                reclaimed = bar["c"] > hi if side == "buy" else bar["c"] < lo
                if reclaimed and j < last_i and not candidate_waiting:
                    # La première clôture de reprise consomme le cycle de cette zone.
                    # Si elle est historique dans le replay courant, elle ne doit
                    # surtout pas être réémise comme nouveau signal sur les bougies suivantes.
                    confirmed_this_zone = True
                    confirmed_zones.append({"c1_t":crt["range_t"],"fvg_t":ft,"setup_type":"fvg"})
                    break
                if reclaimed and j == last_i and not candidate_waiting:
                    # Une FVG plus récente encore active garde la priorité :
                    # une zone plus ancienne ne peut pas déclencher l'entrée
                    # pendant que la candidate récente attend toujours son cycle.
                    test_i, test = tested
                    entry = last["c"]
                    av = atr(m15c) or abs(entry)*0.0005
                    buffer = av*SL_ATR_MULT
                    sl = test["l"]-buffer if side == "buy" else test["h"]+buffer
                    tp, tp_label, tp_meta = nearest_liquidity(entry,h4c,side,crt)
                    if tp is None or (tp <= entry if side=="buy" else tp>=entry):
                        # TP non exploitable pour cette zone : laisser les zones suivantes être évaluées.
                        candidate_rejected = True; rejected_this_zone = True; rejected_meta = {"c1_t":crt["range_t"],"fvg_t":ft}
                        break
                    risk=abs(entry-sl)
                    if risk<=0:
                        candidate_rejected = True; rejected_this_zone = True; rejected_meta = {"c1_t":crt["range_t"],"fvg_t":ft}
                        break
                    rr=abs(tp-entry)/risk
                    fvg_signal_candidate = {"code":"signal","mode":"fvg","side":side,"entry":entry,"sl":sl,"tp":tp,
                        "tp_label":tp_label,"tp_meta":tp_meta,"rr":rr,"c1_t":crt["range_t"],
                        "c1_high":crt["h"],"c1_low":crt["l"],
                        "expansion_t":crt["expansion_t"],"expansion_o":crt["expansion_o"],
                        "expansion_high":crt["expansion_high"],"expansion_low":crt["expansion_low"],
                        "expansion_c":crt["expansion_c"],
                        "fvg_t":ft,"fvg_bottom":lo,"fvg_top":hi,
                        "fvg_state":"confirmed_retest_reclaim","test_t":test["t"],"test_o":test["o"],
                        "test_h":test["h"],"test_l":test["l"],"test_c":test["c"],
                        "confirmation_t":last["t"],"confirmation_o":last["o"],"confirmation_h":last["h"],
                        "confirmation_l":last["l"],"confirmation_c":last["c"],
                        "reentry_t":crt["sweep_t"],"reentry_o":crt["sweep_o"],"reentry_h":crt["sweep_high"],
                        "reentry_l":crt["sweep_low"],"reentry_c":crt["sweep_c"],"sweep_t":crt["sweep_t"],
                        "sweep_low":crt["sweep_low"],"sweep_high":crt["sweep_high"],
                        "trail_trigger":crt["h"] if side=="buy" else crt["l"],
                        "invalidated_zones":invalidated_zones,"confirmed_zones":confirmed_zones}
                    break
        if fvg_signal_candidate:
            break
        if invalidated:
            invalidated_zones.append({"c1_t": crt["range_t"], "fvg_t": ft, "setup_type": "fvg"})
        if not invalidated and not rejected_this_zone and not confirmed_this_zone and not candidate_waiting:
            # Les zones sont parcourues de la plus récente à la plus ancienne :
            # ne jamais laisser une zone plus ancienne écraser le premier candidat pending.
            candidate_waiting = True
            candidate_waiting_meta = {"c1_t": crt["range_t"], "fvg_t": ft, "fvg_bottom": lo, "fvg_top": hi,
                "setup_type": "fvg", "expansion_t":crt["expansion_t"], "expansion_c":crt["expansion_c"]}
    # Reversal lifecycle audit: identify same-side IFVGs from opposite-direction FVGs that
    # have closed through their zone and then been retested/reclaimed on a later candle.
    opposite = fvg_zones(post, "sell" if side=="buy" else "buy")
    if opposite:
        for idx, lo, hi, ft in reversed(opposite):
            flipped = None; test = None
            zone_waiting_meta = None
            confirmed_ifvg_this_zone = False
            for j in range(idx+1, len(post)):
                bar=post[j]
                cross = bar["c"]>hi if side=="buy" else bar["c"]<lo
                if flipped is None and cross:
                    flipped=j
                    # Le candidat IFVG existe dès la clôture de flip.
                    # Une zone plus récente déjà retournée garde la priorité,
                    # même avant son premier retest.
                    zone_waiting_meta = {"c1_t":crt["range_t"],"fvg_t":ft,
                        "fvg_bottom":lo,"fvg_top":hi,"setup_type":"ifvg",
                        "expansion_t":crt["expansion_t"],"expansion_c":crt["expansion_c"],
                        "ifvg_candidate":True,"ifvg_flip_t":bar["t"]}
                    continue
                if flipped is not None and j>flipped:
                    # Après le flip, une clôture qui retraverse entièrement
                    # la zone dans le sens opposé annule l'IFVG avant son retest.
                    flip_invalidated = bar["c"] < lo if side=="buy" else bar["c"] > hi
                    if flip_invalidated:
                        invalidated_zones.append({"c1_t":crt["range_t"], "fvg_t":ft, "setup_type":"ifvg"})
                        zone_waiting_meta = None
                        break
                    touch=bar["l"]<=hi and bar["h"]>=lo
                    if test is None and touch:
                        test=(j,bar)
                        # Le retest est mémorisé même s'il survient sur la dernière
                        # bougie disponible : le cycle attend alors un reclaim futur.
                        zone_waiting_meta = {**(zone_waiting_meta or {}),
                            "ifvg_test_t":bar["t"]}
                        continue
                    if test is not None and j>test[0]:
                        reclaim=bar["c"]>hi if side=="buy" else bar["c"]<lo
                        if reclaim and j < last_i:
                            # Une IFVG est consommée à sa première clôture de reprise,
                            # même si cette clôture est déjà passée dans le replay.
                            confirmed_ifvg_this_zone = True
                            confirmed_zones.append({"c1_t":crt["range_t"],"fvg_t":ft,"setup_type":"ifvg"})
                            zone_waiting_meta = None
                            break
                        if reclaim and j==last_i:
                            # IFVG autonome : inversion (clôture de flip), retest,
                            # puis confirmation sur une clôture distincte.
                            test_i, test_bar = test
                            entry = bar["c"]
                            av = atr(m15c) or abs(entry)*0.0005
                            buffer = av*SL_ATR_MULT
                            sl = test_bar["l"]-buffer if side=="buy" else test_bar["h"]+buffer
                            tp, tp_label, tp_meta = nearest_liquidity(entry,h4c,side,crt)
                            if tp is None or (tp <= entry if side=="buy" else tp>=entry):
                                # Candidate IFVG rejetée : essayer les zones IFVG suivantes.
                                candidate_rejected = True; rejected_meta = {"c1_t":crt["range_t"],"fvg_t":ft,"setup_type":"ifvg"}
                                zone_waiting_meta = None
                                break
                            risk=abs(entry-sl)
                            if risk<=0:
                                candidate_rejected = True; rejected_meta = {"c1_t":crt["range_t"],"fvg_t":ft,"setup_type":"ifvg"}
                                zone_waiting_meta = None
                                break
                            rr=abs(tp-entry)/risk
                            proposed_ifvg_signal = {"code":"signal","mode":"ifvg","setup_type":"ifvg","side":side,
                                "entry":entry,"sl":sl,"tp":tp,"tp_label":tp_label,"tp_meta":tp_meta,"rr":rr,
                                "c1_t":crt["range_t"],"c1_high":crt["h"],"c1_low":crt["l"],
                                "fvg_t":ft,"fvg_bottom":lo,"fvg_top":hi,
                                "expansion_t":crt["expansion_t"],"expansion_o":crt["expansion_o"],
                                "expansion_high":crt["expansion_high"],"expansion_low":crt["expansion_low"],
                                "expansion_c":crt["expansion_c"],"fvg_state":"confirmed_ifvg_retest_reclaim",
                                "ifvg_flip_t":post[flipped]["t"],"test_t":test_bar["t"],"test_o":test_bar["o"],
                                "test_h":test_bar["h"],"test_l":test_bar["l"],"test_c":test_bar["c"],
                                "confirmation_t":bar["t"],"confirmation_o":bar["o"],"confirmation_h":bar["h"],
                                "confirmation_l":bar["l"],"confirmation_c":bar["c"],
                                "reentry_t":crt["sweep_t"],"reentry_o":crt["sweep_o"],
                                "reentry_h":crt["sweep_high"],"reentry_l":crt["sweep_low"],
                                "reentry_c":crt["sweep_c"],"sweep_t":crt["sweep_t"],
                                "sweep_low":crt["sweep_low"],"sweep_high":crt["sweep_high"],
                                "trail_trigger":crt["h"] if side=="buy" else crt["l"],
                                "invalidated_zones":invalidated_zones,"confirmed_zones":confirmed_zones}
                            proposed_activation = int(proposed_ifvg_signal.get("ifvg_flip_t") or 0)
                            current_activation = int((ifvg_signal_candidate or {}).get("ifvg_flip_t") or 0)
                            if ifvg_signal_candidate is None or proposed_activation > current_activation:
                                ifvg_signal_candidate = proposed_ifvg_signal
                            zone_waiting_meta = None
            # Examiner toutes les zones : l'ordre de création d'une FVG ne
            # garantit pas l'ordre auquel leurs flips IFVG surviennent.
            if zone_waiting_meta and not confirmed_ifvg_this_zone:
                proposed_activation = int(zone_waiting_meta.get("ifvg_flip_t") or 0)
                current_activation = int((ifvg_waiting_meta or {}).get("ifvg_flip_t") or 0)
                if ifvg_waiting_meta is None or proposed_activation > current_activation:
                    ifvg_waiting_meta = zone_waiting_meta
    # Arbitrage unique FVG/IFVG : comparer l'instant d'activation de chaque
    # zone (création pour FVG, clôture de flip pour IFVG). Une zone active plus
    # récente bloque toute confirmation issue d'une zone plus ancienne.
    contenders = []
    if fvg_signal_candidate:
        contenders.append((int(fvg_signal_candidate.get("fvg_t") or 0), "signal", fvg_signal_candidate))
    if ifvg_signal_candidate:
        contenders.append((int(ifvg_signal_candidate.get("ifvg_flip_t") or 0), "signal", ifvg_signal_candidate))
    if candidate_waiting_meta:
        contenders.append((int(candidate_waiting_meta.get("fvg_t") or 0), "waiting", candidate_waiting_meta))
    if ifvg_waiting_meta:
        contenders.append((int(ifvg_waiting_meta.get("ifvg_flip_t") or 0), "waiting", ifvg_waiting_meta))
    if contenders:
        _, state, chosen = max(contenders, key=lambda x: (x[0], 1 if x[2].get("setup_type") == "ifvg" else 0))
        if state == "signal":
            chosen["invalidated_zones"] = invalidated_zones
            chosen["confirmed_zones"] = confirmed_zones
            return chosen
        return {"code":"waiting_retrace", **chosen, "invalidated_zones": invalidated_zones, "confirmed_zones": confirmed_zones}
    # Sans zone encore active, une candidate rejetée peut être classée rr_low.
    if candidate_rejected:
        return {"code":"rr_low", **(rejected_meta or {"c1_t":crt["range_t"]}), "invalidated_zones": invalidated_zones, "confirmed_zones": confirmed_zones}
    return {"code":"waiting_retrace", "c1_t":crt["range_t"],
            "expansion_t":crt["expansion_t"],"expansion_c":crt["expansion_c"],
            "invalidated_zones": invalidated_zones, "confirmed_zones": confirmed_zones}

def analyze_core(d1c,h4c,h4f,m15c,m15f,biases=None,only=None,mode=None,min_rr=None):
    if len(h4c)<30 or len(m15c)<30:
        return {"resume":"historique insuffisant"},None
    bias=structure_bias(h4c,SWING_N)
    side="buy" if bias=="up" else "sell" if bias=="down" else None
    if side is None: return {"resume":"structure H4 indéterminée; pas de setup"},None
    if not side_allowed(only,side): return {"resume":"restriction directionnelle du marché"},None
    res=_find_side_signal(side,h4c,h4f,m15c,d1c,mode or "fvg")
    msg={"no_range":"historique insuffisant","no_crt":"pas de CRT H4 valide","no_reentry":"réintégration M15 non confirmée","no_fvg":"aucun FVG M15 valide","waiting_retrace":"FVG M15 en attente de rejet","signal":"SETUP ADONIS ATLAS"}
    info={"resume":"H4 %s · %s"%(ARROW[bias],msg.get(res.get("code"),"analyse ATLAS")),"code":res.get("code"),"c1_t":res.get("c1_t"),"fvg_t":res.get("fvg_t"),"rr":res.get("rr"),"setup_type":res.get("setup_type"),"fvg_bottom":res.get("fvg_bottom"),"fvg_top":res.get("fvg_top"),"fvg_state":res.get("fvg_state"),"expansion_t":res.get("expansion_t"),
        "expansion_c":res.get("expansion_c"),"ifvg_candidate":res.get("ifvg_candidate"),"ifvg_flip_t":res.get("ifvg_flip_t"),"ifvg_test_t":res.get("ifvg_test_t"),"ifvg_confirmation_t":res.get("ifvg_confirmation_t"),"invalidated_zones":res.get("invalidated_zones", [])}
    if res.get("code")!="signal": return info,None
    return info,res

def analyze(sym,d1,h4,m15,now):
    if m15 and now-(m15[-1]["t"]+900)>1800: return {"resume":"marché fermé ou données anciennes"},None
    h4c,h4f=split_closed(h4,14400,now); m15c,m15f=split_closed(m15,900,now)
    info,sig=analyze_core([],h4c,h4f,m15c,m15f,None,SIDE_ONLY.get(sym))
    if sig:
        grade,reason=classify_setup(sym,sig); sig["grade"]=grade; sig["grade_reason"]=reason
        info["grade"]=grade; info["grade_reason"]=reason; info["resume"]+=" · "+grade
    return info,sig

def build_text(sym,sig):
    buy=sig["side"]=="buy"
    return "\n".join(["%s %s — ADONIS ATLAS"%("ACHAT" if buy else "VENTE",label(sym)),
      "CRT H4 2 bougies : range → sweep/réinsertion · recherche M15 immédiate, sans attente d’expansion H4",
      "%s M15 post-réintégration · entrée clôture M15 : %s"%("IFVG inversé puis retest-confirmé" if sig.get("mode")=="ifvg" else "FVG retest-confirmé",fmt(sig["entry"])),
      "SL derrière la mèche du test M15 + buffer ATR(14) × %.3f : %s"%(SL_ATR_MULT,fmt(sig["sl"])),
      "TP : %s (%s)"%(fmt(sig["tp"]),sig["tp_label"]),"RR indicatif : %.2f"%sig["rr"],
      "Structure H4, price action CRT et liquidités; aucun ordre n'est passé automatiquement.",
      "Vérifie le prix et le spread sur MT5 avant toute exécution."])


# ==================================================================
#  SCANNER (récupère les bougies et envoie les alertes)
# ==================================================================
class ApiError(Exception):
    pass


_REQ = 0
# Un seul appel public Deriv à la fois, même si le scanner et le backtest
# possèdent chacun leur socket. Cela évite les lectures concurrentes et les
# rafales de requêtes qui aggravent les réponses de limitation (429).
_DERIV_REQUEST_LOCK = asyncio.Lock()
_DERIV_CONNECT_LOCK = asyncio.Lock()
_DERIV_LAST_REQUEST = 0.0
_DERIV_MIN_INTERVAL = 0.35  # cadence commune scanner + backtest


async def connect_public_ws(http, attempts=6):
    """Ouvre le WS public avec attente progressive en cas de HTTP 429."""
    async with _DERIV_CONNECT_LOCK:
        for attempt in range(attempts):
            try:
                return await http.ws_connect(PUBLIC_WS, heartbeat=25)
            except aiohttp.WSServerHandshakeError as e:
                if e.status != 429 or attempt >= attempts - 1:
                    raise
                retry_after = None
                try:
                    retry_after = float(e.headers.get("Retry-After", ""))
                except (TypeError, ValueError, AttributeError):
                    pass
                delay = min(60.0, retry_after if retry_after and retry_after > 0 else 5.0 * (2 ** attempt))
                print("Deriv WS : HTTP 429, nouvelle tentative dans %.0f s (%d/%d)" %
                      (delay, attempt + 1, attempts - 1))
                await asyncio.sleep(delay)


async def ws_request(ws, payload, timeout=15):
    global _REQ, _DERIV_LAST_REQUEST
    # Le verrou couvre l'envoi ET la réception : aucune autre coroutine ne
    # peut consommer par erreur la réponse req_id de cette requête.
    async with _DERIV_REQUEST_LOCK:
        wait = _DERIV_MIN_INTERVAL - (time.monotonic() - _DERIV_LAST_REQUEST)
        if wait > 0:
            await asyncio.sleep(wait)
        _DERIV_LAST_REQUEST = time.monotonic()
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
        return False
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as http:
            async with http.post(
                "https://ntfy.sh/" + NTFY_TOPIC,
                data=text.encode("utf-8"),
                headers={"Title": title, "Priority": "high", "Tags": "chart_with_upwards_trend"},
            ) as r:
                if 200 <= r.status < 300:
                    return True
                print("ntfy a répondu", r.status)
                return False
    except Exception as e:
        print("Erreur ntfy :", repr(e))
        return False


async def handle_signal(sym, sig):
    # Dedup en mémoire uniquement : aucun volume persistant requis. Garder une
    # fenêtre bornée sans vider brutalement les clés les plus récentes.
    key = (sym, sig["side"], sig["c1_t"], sig["fvg_t"])
    if key in SENT:
        return
    text = build_text(sym, sig)
    title = "Setup %s %s %s" % (sig.get("grade", "B"), label(sym), "ACHAT" if sig["side"] == "buy" else "VENTE")
    # Ne marquer comme envoyé qu'après réussite HTTP; en cas d'échec, le scan
    # suivant pourra réessayer. En mémoire, les alertes réussies sont bornées.
    if not NTFY_TOPIC:
        SIGNALS.append({"heure": time.strftime("%H:%M:%S", time.gmtime()) + " UTC", "texte": text})
        SENT.add(key)
        while len(SENT) > 2000:
            SENT.pop()
        print("SIGNAL local (ntfy non configuré)", sym, sig["side"])
        return
    ok = await send_ntfy(title, text)
    if ok:
        SENT.add(key)
        while len(SENT) > 2000:
            SENT.pop()
        SIGNALS.append({"heure": time.strftime("%H:%M:%S", time.gmtime()) + " UTC", "texte": text})
        print("SIGNAL envoyé", sym, sig["side"])
    else:
        print("Signal ntfy non confirmé; retry au prochain scan :", sym, key)


def stale(fetched, now, ttl, gran):
    return now - fetched > ttl or int(now // gran) != int(fetched // gran)


async def scan_once(ws, cache):
    for sym in SYMBOLS:
        try:
            now = time.time()
            hk = (sym, 14400)
            if hk not in cache or stale(cache[hk][0], now, H4_TTL, 14400):
                fresh_h4 = await fetch_candles(ws, real(sym), 14400, 1500)
                # Ne jamais perdre un CRT ancien encore valide lors du rafraîchissement:
                # on fusionne les nouvelles bougies avec tout le contexte déjà chargé.
                previous_h4 = cache.get(hk, (None, []))[1]
                merged_h4 = {c["t"]: c for c in previous_h4}
                merged_h4.update({c["t"]: c for c in fresh_h4})
                cache[hk] = (now, sorted(merged_h4.values(), key=lambda c: c["t"]))
            m15 = await fetch_candles(ws, real(sym), 900, 1500)
            info, sig = analyze(sym, [], cache[hk][1], m15, time.time())
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
        # Le backtest a la priorité : le scanner n'envoie aucune requête
        # Deriv tant que l'historique est en cours de téléchargement.
        if BT["running"]:
            await asyncio.sleep(5)
            continue
        try:
            if ws is None or ws.closed:
                if http is None or http.closed:
                    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, connect=15))
                ws = await connect_public_ws(http, attempts=4)
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


def _clamp_trailing_sl(buy, old_sl, candidate_sl, tp):
    """SL protecteur monotone, sans jamais franchir le TP de référence."""
    if buy:
        return min(max(old_sl, candidate_sl), tp)
    return max(min(old_sl, candidate_sl), tp)


def simulate(sig, m15, i, max_bars=288):
    """SL/TP d'abord (SL prioritaire); gap-aware; trailing après retest puis
    confirmation sur une bougie ultérieure. Retourne aussi un journal d'événements."""
    buy = sig["side"] == "buy"
    entry, sl, tp = float(sig["entry"]), float(sig["sl"]), float(sig["tp"])
    risk = abs(entry-sl)
    if risk <= 0 or i < 0 or i >= len(m15):
        return "invalid", 0.0, i, []
    trigger = sig.get("trail_trigger")
    armed = False
    zones = []
    journal = []
    last_j = min(len(m15)-1, i+max_bars)

    def stop_result(price):
        r = ((price-entry)/risk) if buy else ((entry-price)/risk)
        return ("win" if r > 1e-12 else ("loss" if r < -1e-12 else "breakeven")), r

    for j in range(i+1, last_j+1):
        c = m15[j]
        # Si le marché ouvre déjà au-delà du TP, l'ordre limite aurait été
        # exécuté dès l'ouverture : ce cas n'est pas une collision intrabougie.
        # On crédite seulement le TP contractuel (pas un meilleur prix supposé).
        tp_gap_at_open = c["o"] >= tp if buy else c["o"] <= tp
        if tp_gap_at_open:
            r = ((tp-entry)/risk) if buy else ((entry-tp)/risk)
            journal.append({"event":"tp", "t":c["t"], "tp":tp,
                            "fill":tp, "gap":True, "execution":"open_beyond_target"})
            return ("win" if r > 0 else "loss"), r, j, journal
        # Stop-market : en cas de gap au-delà du SL, simuler le prix d'ouverture.
        stop_hit = c["l"] <= sl if buy else c["h"] >= sl
        if stop_hit:
            fill = (c["o"] if (c["o"] < sl if buy else c["o"] > sl) else sl)
            label_, r = stop_result(fill)
            journal.append({"event":"stop", "t":c["t"], "sl":sl, "fill":fill,
                           "gap":fill != sl, "ambiguous_sl_tp":bool(c["h"] >= tp and c["l"] <= sl) if buy else bool(c["l"] <= tp and c["h"] >= sl)})
            return label_, r, j, journal
        if (c["h"] >= tp if buy else c["l"] <= tp):
            r = ((tp-entry)/risk) if buy else ((entry-tp)/risk)
            journal.append({"event":"tp", "t":c["t"], "tp":tp, "fill":tp,
                            "gap":False, "execution":"intrabar_target"})
            return ("win" if r > 0 else "loss"), r, j, journal

        if not armed and trigger is not None:
            armed = c["c"] > trigger if buy else c["c"] < trigger
            if armed:
                zones.clear()
                journal.append({"event":"trailing_armed", "t":c["t"], "trigger":trigger})
                continue
        if not armed or j < 2:
            continue

        # FVG fraîche créée après armement; aucune zone n'est utilisable
        # avant la bougie de création.
        a = m15[j-2]
        if buy and c["l"] > a["h"]:
            zones.append({"lo":a["h"], "hi":c["l"], "idx":j, "test":None})
        elif not buy and c["h"] < a["l"]:
            zones.append({"lo":c["h"], "hi":a["l"], "idx":j, "test":None})

        # Une seule FVG de trailing possède la priorité : la plus récente
        # encore active. Les anciennes restent en mémoire, mais ne peuvent
        # déplacer le SL tant que la candidate récente n'est pas confirmée
        # ou invalidée. Après invalidation, l'ancienne ne sera réévaluée
        # qu'à partir de la bougie suivante (pas de bascule intrabougie).
        if zones:
            z = zones[-1]
            if j > z["idx"]:
                # Une FVG de trailing invalidée par clôture au-delà de son bord
                # opposé ne doit plus servir à déplacer le SL lors d'un reclaim ultérieur.
                invalidated = c["c"] < z["lo"] if buy else c["c"] > z["hi"]
                if invalidated:
                    zones.pop()
                    journal.append({"event":"trailing_fvg_invalidated", "t":c["t"], "zone":[z["lo"],z["hi"]]})
                else:
                    touched = c["l"] <= z["hi"] and c["h"] >= z["lo"]
                    if z["test"] is None and touched:
                        z["test"] = {"t":c["t"], "l":c["l"], "h":c["h"]}
                        journal.append({"event":"trailing_fvg_test", "t":c["t"], "zone":[z["lo"],z["hi"]]})
                    # Confirmation distinct de la bougie de retest.
                    elif z["test"] is not None and c["t"] > z["test"]["t"]:
                        confirmed = c["c"] > z["hi"] if buy else c["c"] < z["lo"]
                        if confirmed:
                            av = atr(m15[max(0, j-20):j+1]) or abs(entry)*0.0005
                            candidate = z["test"]["l"]-av*SL_ATR_MULT if buy else z["test"]["h"]+av*SL_ATR_MULT
                            # Le trailing ne doit jamais placer le SL au-delà de la cible H4.
                            new_sl = _clamp_trailing_sl(buy, sl, candidate, tp)
                            if new_sl != sl:
                                journal.append({"event":"trailing_sl_move", "t":c["t"], "old_sl":sl,
                                               "new_sl":new_sl, "test_t":z["test"]["t"], "confirmation_t":c["t"]})
                                sl = new_sl
                            zones.pop()

    close = m15[last_j]["c"]
    r = ((close-entry) if buy else (entry-close))/risk
    return ("timeout" if i+max_bars < len(m15) else "open"), r, last_j, journal


FUNNEL_RR = 2.0

def _classify_funnel_event(st, event_key, rr_v):
    """Count each confirmed event once and assign an exclusive RR bucket on first observation."""
    st["confirmed"].add(event_key)
    if event_key in st["rr"] or event_key in st["rr_ge2"]:
        return
    if rr_v is None:
        return
    if rr_v < FUNNEL_RR:
        st["rr"].add(event_key)
    else:
        st["rr_ge2"].add(event_key)


def _closed_h4_prefix_count(h4_open_times, m15_close_t):
    """Nombre de bougies H4 clôturées et disponibles à la clôture M15 T."""
    return bisect_right(h4_open_times, int(m15_close_t) - 14400)


async def replay(d1, h4, m15, start_i, only=None, mode="fvg", thresholds=None):
    thresholds = tuple(thresholds or (MIN_RR,))
    lo_rr = min(thresholds)
    # D1 est conservé dans le rapport de couverture, mais n'intervient pas
    # dans les signaux ATLAS : la direction et le TP reposent sur H4.
    h4_t = [int(c["t"]) for c in h4]
    if any(b <= a for a, b in zip(h4_t, h4_t[1:])):
        raise ValueError("Historique H4 non strictement croissant ou timestamps dupliqués")
    cache = {}
    seen = {t: set() for t in thresholds}
    trades = {t: [] for t in thresholds}
    busy = {t: -1 for t in thresholds}
    st = {"steps": 0, "bias": 0, "zone": 0, "crt": set(), "fvg": set(), "rr": set(),
          "waiting_confirmation": set(), "confirmed": set(), "rr_ge2": set(),
          "rr_uncomputable": set(), "zone_states": {}, "no_crt_steps": 0, "no_fvg_steps": 0,
          "body_medium_excluded": set(), "h4_buy": 0, "h4_sell": 0,
          "direction_blocked": 0, "direction_blocked_buy": 0, "direction_blocked_sell": 0}
    for i in range(start_i, len(m15)):
        if i % 200 == 0:
            await asyncio.sleep(0)  # laisse respirer le serveur
        T = m15[i]["t"] + 900
        kh = _closed_h4_prefix_count(h4_t, T)
        # Garde anti-lookahead explicite et contrôle du bord exact de clôture.
        if kh and h4_t[kh - 1] + 14400 > T:
            raise RuntimeError("Look-ahead H4 détecté : bougie visible non clôturée")
        if kh < len(h4_t) and h4_t[kh] + 14400 <= T:
            raise RuntimeError("Décalage H4 : bougie clôturée exclue du replay")
        if kh < 15:
            continue
        key = kh
        if key not in cache:
            cache[key] = (None, structure_bias(h4[:kh], SWING_N))
        b_d1, b_h4 = cache[key]
        st["steps"] += 1
        bias_side = pick_side(b_d1, b_h4)  # "up" / "down" / None
        side = "buy" if bias_side == "up" else "sell" if bias_side == "down" else None
        if side == "buy":
            st["h4_buy"] += 1
        elif side == "sell":
            st["h4_sell"] += 1
        if side is None:
            continue
        if not side_allowed(only, side):
            st["direction_blocked"] += 1
            st["direction_blocked_buy" if side == "buy" else "direction_blocked_sell"] += 1
            continue
        st["bias"] += 1
        # Détection structurelle indépendante de la disponibilité du portefeuille.
        # L'occupation ne filtre que la couche simulation/exécution ci-dessous.
        free = [t for t in thresholds if i > busy[t]]
        d1c = []  # volontairement vide : aucune règle de stratégie ne lit D1
        h4c = h4[:kh]  # CRT persistant sur tout le H4 disponible
        m15c = m15[:i + 1]  # chronologie complète depuis les données chargées; CRT ancien non tronqué
        h4f = partial_candle(m15, i, T, 14400)
        info, sig = analyze_core(d1c, h4c, h4f, m15c, None, (b_d1, b_h4), only, mode, 0.0)
        rk = RANK.get(info.get("code"), -1)
        code = info.get("code")
        if code == "no_crt":
            st["no_crt_steps"] += 1
        elif code == "no_fvg":
            st["no_fvg_steps"] += 1
        if mode == "fvg" and info.get("code") == "reentry_body_medium":
            if info.get("c1_t") is not None:
                st["body_medium_excluded"].add(info["c1_t"])
            continue
        if rk >= RANK["no_crt"]:
            st["zone"] += 1
        # Compter un CRT avec sweep uniquement après la détection effective
        # du sweep M15. Le code "no_sweep" signifie précisément qu'il manque.
        if rk > RANK["no_sweep"] and info.get("c1_t"):
            st["crt"].add(info["c1_t"])
        # Les premières confirmations historiques doivent aussi fermer le cycle
        # de la zone dans le funnel, même si le moteur n'émet plus de signal courant.
        for done in info.get("confirmed_zones", []):
            done_key = (done.get("c1_t"), done.get("fvg_t"), done.get("setup_type", "fvg"))
            if done_key[0] and done_key[1]:
                st["fvg"].add(done_key)
                if st["zone_states"].get(done_key) != "invalidated":
                    st["zone_states"][done_key] = "confirmed"
        # Les invalidations explicites ont la priorité sur les états non invalidés.
        for dead in info.get("invalidated_zones", []):
            dead_key = (dead.get("c1_t"), dead.get("fvg_t"), dead.get("setup_type", "fvg"))
            if dead_key[0] and dead_key[1]:
                st["fvg"].add(dead_key)
                st["zone_states"][dead_key] = "invalidated"
        zone_key = (info.get("c1_t"), info.get("fvg_t"), info.get("setup_type", "fvg"))
        if rk >= RANK["waiting_retrace"] and zone_key[0] and zone_key[1]:
            st["fvg"].add(zone_key)
        if zone_key[0] and zone_key[1] and code in ("waiting_retrace", "rr_low", "signal"):
            # Snapshot final/last observed state per zone, rather than accumulating
            # a zone in pending and then again in its terminal state.
            zone_state = {"waiting_retrace": "pending", "rr_low": "rejected_tp_risk",
                          "signal": "confirmed"}[code]
            previous_state = st["zone_states"].get(zone_key)
            if previous_state not in ("confirmed", "invalidated"):
                st["zone_states"][zone_key] = zone_state
        if code == "waiting_retrace" and zone_key[0] and zone_key[1]:
            st["waiting_confirmation"].add(zone_key)
        if code == "rr_low" and zone_key[0] and zone_key[1]:
            st["rr_uncomputable"].add(zone_key)
        # L'événement confirmé est la confirmation datée, pas simplement la zone.
        if code == "signal" and zone_key[0] and zone_key[1]:
            event_key = (zone_key[0], zone_key[1], sig.get("confirmation_t", T), sig.get("side"), sig.get("setup_type", "fvg"))
            rr_v = sig.get("rr") if sig else info.get("rr")
            _classify_funnel_event(st, event_key, rr_v)
        if not sig or not free:
            continue
        k = (mode, sig.get("setup_type", "fvg"), sig["side"], sig["c1_t"],
             sig.get("fvg_t"), sig.get("reentry_t"), sig.get("confirmation_t"))
        outcome = None
        for thr in free:
            if sig["rr"] >= thr and k not in seen[thr]:
                seen[thr].add(k)
                if outcome is None:
                    outcome = simulate(sig, m15, i)
                res, r, j, sim_journal = outcome
                exit_t = int(m15[j]["t"] + 900) if 0 <= j < len(m15) else None
                range_low = sig.get("c1_low")
                range_high = sig.get("c1_high")
                sweep_low = sig.get("sweep_low")
                sweep_high = sig.get("sweep_high")
                if sig.get("side") == "buy" and range_low is not None and sweep_low is not None:
                    sweep_depth = max(0.0, float(range_low) - float(sweep_low))
                elif sig.get("side") == "sell" and range_high is not None and sweep_high is not None:
                    sweep_depth = max(0.0, float(sweep_high) - float(range_high))
                else:
                    sweep_depth = None
                range_size = (abs(float(range_high) - float(range_low))
                              if range_high is not None and range_low is not None else None)
                sweep_depth_ratio = (sweep_depth / range_size
                                     if sweep_depth is not None and range_size is not None and range_size > 0 else None)
                trades[thr].append({
                    "sym": None, "side": sig["side"], "t": T,
                    "signal_t": T, "exit_t": exit_t,
                    "res": res, "r": round(float(r), 5), "rr": round(float(sig["rr"]), 5),
                    "entry": sig.get("entry"), "sl": sig.get("sl"), "tp": sig.get("tp"),
                    "tp_label": sig.get("tp_label"), "tp_liquidity_selected": sig.get("tp_meta"),
                    "mode": mode, "setup_type": sig.get("setup_type", "fvg"),
                    "sweep_t": sig.get("sweep_t"),
                    "crt_t": sig.get("c1_t"), "reentry_t": sig.get("reentry_t"),
                    "reentry_o": sig.get("reentry_o"), "reentry_h": sig.get("reentry_h"),
                    "reentry_l": sig.get("reentry_l"), "reentry_c": sig.get("reentry_c"),
                    "sweep_low": sig.get("sweep_low"), "sweep_high": sig.get("sweep_high"), "range_high": sig.get("c1_high"),
                    "range_low": sig.get("c1_low"), "fvg_t": sig.get("fvg_t"),
                    "sweep_depth": sweep_depth,
                    "range_size": range_size,
                    "sweep_depth_ratio": sweep_depth_ratio,
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
                    "test_t_m15": sig.get("test_t"), "test_o_m15": sig.get("test_o"),
                    "test_h_m15": sig.get("test_h"), "test_l_m15": sig.get("test_l"),
                    "test_c_m15": sig.get("test_c"),
                    "sweep_o_h4": sig.get("reentry_o"), "sweep_h_h4": sig.get("reentry_h"),
                    "sweep_l_h4": sig.get("reentry_l"), "sweep_c_h4": sig.get("reentry_c"),
                    "confirmation_t_m15": sig.get("confirmation_t"),
                    "confirmation_o_m15": sig.get("confirmation_o"),
                    "confirmation_h_m15": sig.get("confirmation_h"),
                    "confirmation_l_m15": sig.get("confirmation_l"),
                    "simulation_events": sim_journal,
                })
                busy[thr] = j
    return trades, st


def _max_concurrent_positions(trades):
    """Maximum de positions simultanées, à partir des heures d'entrée/sortie.
    Une sortie à T est traitée avant une entrée à T.
    """
    events = []
    for t in trades:
        # Les positions encore ouvertes comptent aussi jusqu'à leur dernière
        # bougie connue (exit_t est renseigné par le simulateur).
        start = t.get("signal_t", t.get("t"))
        end = t.get("exit_t")
        if start is None or end is None or int(end) < int(start):
            continue
        events.append((int(start), 1))
        events.append((int(end), -1))
    # -1 avant +1 en cas d'égalité : positions fermées avant les nouvelles entrées.
    events.sort(key=lambda x: (x[0], x[1]))
    active = peak = 0
    for _, delta in events:
        active += delta
        peak = max(peak, active)
    return peak


def performance_stats(trades):
    """Mesures réalisées par heure de sortie; les sorties simultanées sont groupées pour le DD."""
    ordered = sorted((t for t in trades if t["res"] != "open"),
                     key=lambda t: (t.get("exit_t") or t.get("t", 0), t.get("t", 0)))
    rs = [float(t["r"]) for t in ordered]
    gross_win = sum(r for r in rs if r > 0)
    gross_loss = -sum(r for r in rs if r < 0)
    # Plusieurs marchés peuvent clôturer au même timestamp. Leur ordre relatif
    # n'est pas observable : calculer le drawdown après agrégation de ce lot
    # évite qu'un tri arbitraire gonfle ou réduise le DD.
    exit_batches = {}
    for trade in ordered:
        exit_t = int(trade.get("exit_t") or trade.get("t", 0))
        exit_batches[exit_t] = exit_batches.get(exit_t, 0.0) + float(trade["r"])
    equity = peak = max_dd = 0.0
    for exit_t in sorted(exit_batches):
        equity += exit_batches[exit_t]
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    # Les séries de gains/pertes restent ordonnées de façon reproductible par
    # (sortie, signal); le rapport documente cette convention en cas d'égalité.
    loss_streak = win_streak = max_loss_streak = max_win_streak = 0
    for r in rs:
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
        "max_positions_simultanees": _max_concurrent_positions(trades),
        "max_risque_initial_simultane_r": float(_max_concurrent_positions(trades)),
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
    stats = performance_stats(trades)
    buy_stats = performance_stats([t for t in trades if t["side"] == "buy"])
    sell_stats = performance_stats([t for t in trades if t["side"] == "sell"])
    return {"symbole": label(sym), "achats": len(buys), "r_achats": round(sum(t["r"] for t in buys), 2),
            "achats_pf": buy_stats["profit_factor"], "achats_dd_r": buy_stats["max_drawdown_r"],
            "achats_expectancy_r": buy_stats["expectancy_r"],
            "ventes": len(sells), "r_ventes": round(sum(t["r"] for t in sells), 2),
            "ventes_pf": sell_stats["profit_factor"], "ventes_dd_r": sell_stats["max_drawdown_r"],
            "ventes_expectancy_r": sell_stats["expectancy_r"], "setups": len(trades),
            "gagnes": w, "perdus": l, "equilibres": be, "expires": to, "ouverts": op, "r_total": round(total_r, 2),
            "profit_factor": stats["profit_factor"], "max_drawdown_r": stats["max_drawdown_r"],
            "max_positions_simultanees": stats["max_positions_simultanees"],
            "max_risque_initial_simultane_r": stats["max_risque_initial_simultane_r"],
            "expectancy_r": stats["expectancy_r"]}


def portfolio_reconciliation(trades, rows):
    """Contrôle indépendant des totaux portefeuille, marchés et états de trade."""
    allowed = {"win", "loss", "breakeven", "timeout", "open"}
    counts = {state: sum(t.get("res") == state for t in trades) for state in allowed}
    invalid_states = sum(t.get("res") not in allowed for t in trades)
    closed = [t for t in trades if t.get("res") != "open"]
    closed_r = sum(float(t.get("r", 0) or 0) for t in closed)
    identities = []
    for t in trades:
        identities.append((t.get("sym"), t.get("side"), t.get("mode"),
                           t.get("setup_type", "fvg"), t.get("crt_t"), t.get("fvg_t"),
                           t.get("reentry_t"),
                           t.get("confirmation_t_m15", t.get("confirmation_t"))))
    duplicate_count = len(identities) - len(set(identities))
    by_symbol = {}
    for t in trades:
        sym = t.get("sym")
        item = by_symbol.setdefault(sym, {"setups": 0, "closes": 0, "r_total": 0.0})
        item["setups"] += 1
        if t.get("res") != "open":
            item["closes"] += 1
            item["r_total"] += float(t.get("r", 0) or 0)
    row_map = {r.get("symbole"): r for r in rows if "erreur" not in r}
    row_count_gap = 0
    row_r_gap = []
    for sym, item in by_symbol.items():
        row = row_map.get(label(sym))
        if row is None:
            row_count_gap += item["setups"]
            row_r_gap.append({"sym": sym, "raison": "ligne marché absente"})
            continue
        if int(row.get("setups", 0)) != item["setups"]:
            row_count_gap += abs(int(row.get("setups", 0)) - item["setups"])
        if abs(float(row.get("r_total", 0) or 0) - item["r_total"]) > 0.011:
            row_r_gap.append({"sym": sym, "ligne_r": row.get("r_total"),
                              "trades_r": round(item["r_total"], 5)})
    row_only = [sym for sym in row_map if sym not in {label(x) for x in by_symbol} and int(row_map[sym].get("setups", 0)) != 0]
    anomalies = []
    if invalid_states:
        anomalies.append("états de trade inconnus")
    if sum(counts.values()) != len(trades):
        anomalies.append("les états ne partitionnent pas les trades")
    if duplicate_count:
        anomalies.append("identités de trades dupliquées")
    if row_count_gap:
        anomalies.append("nombre de trades différent entre lignes marché et portefeuille")
    if row_r_gap:
        anomalies.append("R par marché différent entre lignes et trades")
    if row_only:
        anomalies.append("lignes marché sans trades correspondants")
    return {"coherent": not anomalies, "anomalies": anomalies,
            "trades_total": len(trades), "etats": counts,
            "etats_inconnus": invalid_states, "trades_clotures": len(closed),
            "trades_ouverts_exclus_du_r_realise": counts["open"],
            "r_realise_trades": round(closed_r, 5),
            "r_realise_lignes_marches": round(sum(float(r.get("r_total", 0) or 0) for r in row_map.values()), 5),
            "identites_dupliquees": duplicate_count,
            "ecart_setups_lignes_vs_trades": row_count_gap,
            "ecarts_r_par_marche": row_r_gap, "lignes_sans_trades": row_only}


def combo_total(trades, window_start, window_end):
    split_t = window_end - (window_end - window_start) / 3
    # Les trades encore ouverts ne sont pas inclus dans les performances réalisées.
    closed = sorted((t for t in trades if t["res"] != "open"), key=lambda t: t["t"])
    old = [t for t in closed if t["t"] < split_t]
    new = [t for t in closed if t["t"] >= split_t]
    total_r = sum(t["r"] for t in closed)
    stats = performance_stats(trades)

    # Trois périodes calendaires fixes sur la fenêtre complète du backtest,
    # y compris les périodes sans aucun trade.
    periods = []
    span = max(1, window_end - window_start)
    for idx in range(3):
        lo = window_start + span * idx / 3
        hi = window_start + span * (idx + 1) / 3
        group = [t for t in closed if lo <= t["t"] < hi or (idx == 2 and t["t"] == window_end)]
        periods.append({"n": len(group), "r": round(sum(t["r"] for t in group), 2)})

    buys = performance_stats([t for t in trades if t["side"] == "buy"])
    sells = performance_stats([t for t in trades if t["side"] == "sell"])
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
            "max_positions_simultanees": stats["max_positions_simultanees"],
            "max_risque_initial_simultane_r": stats["max_risque_initial_simultane_r"],
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
            message = str(e).lower()
            if ("rate" in message or "limit" in message or "too many" in message) and attempt < 4:
                delay = min(60, 15 * (2 ** attempt))
                BT["progress"] = "limite de Deriv atteinte, pause de %d s..." % delay
                await asyncio.sleep(delay)
                continue
            raise


async def fetch_history(ws, sym, gran, target, page, diagnostic_key=None):
    """Télécharge l'historique; conserve la clé demandée même si Deriv résout un alias."""
    diag_sym = diagnostic_key or sym
    got = {}
    end = "latest"
    stop_reason = "max_pages_100"
    previous_oldest = None
    page_audit = []
    max_pages = 100

    for page_no in range(1, max_pages + 1):
        requested_end = end
        try:
            data = await hist_request(ws, {"ticks_history": sym, "end": end, "style": "candles",
                                           "granularity": gran, "count": page})
        except Exception as exc:
            BT_HISTORY_DIAGNOSTICS[(diag_sym, gran)] = {
                "pages": page_audit, "stop_reason": "erreur_api",
                "error_type": type(exc).__name__, "error": str(exc)[:180],
                "target": target, "unique_candles": len(got), "complete": False}
            print("[HIST-DIAG] ERREUR sym=%s gran=%s page=%d end=%s type=%s erreur=%r" %
                  (sym, gran, page_no, requested_end, type(exc).__name__, exc), flush=True)
            raise

        raw = data.get("candles")
        cs = norm_candles(raw)
        fresh = [c for c in cs if c["t"] not in got]
        for c in cs:
            got[c["t"]] = c

        oldest = cs[0]["t"] if cs else None
        newest = cs[-1]["t"] if cs else None
        page_audit.append({"page": page_no, "end_demande": requested_end,
                           "demande": page, "brutes": len(raw or []),
                           "normalisees": len(cs), "nouvelles": len(fresh),
                           "doublons": max(0, len(cs) - len(fresh)),
                           "oldest": oldest, "newest": newest})
        print("[HIST-DIAG] sym=%s gran=%s page=%d/%d end_demande=%s demande=%d brutes=%d normalisees=%d nouvelles=%d total_unique=%d oldest=%s newest=%s" %
              (sym, gran, page_no, max_pages, requested_end, page, len(raw or []), len(cs), len(fresh),
               len(got), oldest, newest), flush=True)

        if not cs:
            stop_reason = "reponse_vide"
            break
        if len(got) >= target:
            stop_reason = "objectif_atteint"
            break
        if previous_oldest is not None and oldest >= previous_oldest:
            stop_reason = "aucune_progression_temporelle"
            break
        if previous_oldest is not None and not fresh:
            stop_reason = "page_entierement_dupliquee"
            break

        previous_oldest = oldest
        end = str(oldest - 1)
        await asyncio.sleep(2.5)

    await asyncio.sleep(2.5)
    result = sorted(got.values(), key=lambda c: c["t"])
    complete = len(result) >= target and stop_reason == "objectif_atteint"
    BT_HISTORY_DIAGNOSTICS[(diag_sym, gran)] = {
        "pages": page_audit, "stop_reason": stop_reason, "target": target,
        "unique_candles": len(result), "complete": complete,
        "oldest": result[0]["t"] if result else None,
        "newest": result[-1]["t"] if result else None,
        "duplicates_total": sum(p["doublons"] for p in page_audit),
        "max_pages": max_pages}
    print("[HIST-DIAG] FIN sym=%s gran=%s total=%d cible=%d pages_max=%d arret=%s debut=%s fin=%s" %
          (sym, gran, len(result), target, max_pages, stop_reason,
           result[0]["t"] if result else None, result[-1]["t"] if result else None), flush=True)
    return result


BT_MODES = ("fvg",)  # ATLAS valide uniquement l’entrée FVG définie par la stratégie
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


def rr_descriptive_breakdown(trades):
    """Ventilation descriptive des RR sur la liste de trades de référence.
    Ne filtre pas les entrées et ne simule pas un portefeuille alternatif.
    """
    bands = ((0.0, 1.0, "RR < 1"), (1.0, 1.5, "1 <= RR < 1.5"),
             (1.5, 2.0, "1.5 <= RR < 2"), (2.0, float("inf"), "RR >= 2"))
    out = []
    for low, high, label_ in bands:
        group = [t for t in trades if t.get("rr") is not None and
                 float(t.get("rr", 0)) >= low and float(t.get("rr", 0)) < high]
        stats = performance_stats(group)
        out.append({"tranche": label_, "trades": len(group),
                    "r_net": round(sum(float(t.get("r", 0) or 0) for t in group
                                        if t.get("res") != "open"), 3), **stats})
    return out


def pack_history_export(h4, m15):
    """Sérialise exactement les bougies clôturées consommées par replay().

    Format compact gzip+base64 pour éviter un JSON de rapport démesuré.
    Le SHA-256 porte sur le JSON canonique décompressé, pas sur le flux gzip.
    """
    payload = json.dumps({"schema": "atlas-ohlc-v1", "h4": h4, "m15": m15},
                         ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    compressed = gzip.compress(payload, compresslevel=6, mtime=0)
    return {"encoding": "gzip+base64", "sha256_json": hashlib.sha256(payload).hexdigest(),
            "bytes_json": len(payload), "bytes_gzip": len(compressed),
            "data": base64.b64encode(compressed).decode("ascii")}


def history_coverage(candles, granularity, window_start, window_end):
    """Couverture observée dans la fenêtre; les fermetures de marché restent à interpréter."""
    inside = [c for c in candles if window_start <= int(c["t"]) < window_end]
    if not inside:
        return {"n": 0, "debut": None, "fin_cloture": None,
                "retard_debut_secondes": None, "retard_fin_secondes": None,
                "couverture_calendaire_pct": 0.0, "trous_calendaires": 0,
                "plus_grand_trou_calendaire_bougies": 0,
                "plus_grand_trou_debut": None, "plus_grand_trou_fin": None,
                "methode": "calendaire brute; fermetures de session non retranchées"}
    ts = sorted(set(int(c["t"]) for c in inside))
    expected = max(1, int((window_end - window_start) // granularity))
    missing = []
    for a, b in zip(ts, ts[1:]):
        gap = max(0, int((b - a) // granularity) - 1)
        if gap:
            missing.append((gap, a + granularity, b))
    largest = max(missing, key=lambda x: x[0]) if missing else (0, None, None)
    return {"n": len(ts), "debut": ts[0], "fin_cloture": ts[-1] + granularity,
            "retard_debut_secondes": max(0, ts[0] - window_start),
            "retard_fin_secondes": max(0, window_end - (ts[-1] + granularity)),
            "couverture_calendaire_pct": round(min(100.0, len(ts) / expected * 100.0), 1),
            "trous_calendaires": len(missing),
            "plus_grand_trou_calendaire_bougies": largest[0],
            "plus_grand_trou_debut": largest[1], "plus_grand_trou_fin": largest[2],
            "methode": "calendaire brute; fermetures de session et jours fériés non retranchés"}


def classify_history_window(cov, h4_diag, m15_diag):
    """Sépare les preuves de pagination des bornes observées; ne suppose aucun calendrier de session."""
    h4 = cov.get("H4", {})
    m15 = cov.get("M15", {})
    h4_bounds = bool(h4.get("n", 0)) and h4.get("retard_debut_secondes") is not None and h4.get("retard_fin_secondes") is not None
    m15_bounds = bool(m15.get("n", 0)) and m15.get("retard_debut_secondes") is not None and m15.get("retard_fin_secondes") is not None
    return {
        "pagination_H4_objectif_atteint": bool(h4_diag.get("complete", False)),
        "pagination_M15_objectif_atteint": bool(m15_diag.get("complete", False)),
        "H4_bornes_observees": h4_bounds,
        "M15_bornes_observees": m15_bounds,
        "retard_debut_H4_secondes": h4.get("retard_debut_secondes"),
        "retard_fin_H4_secondes": h4.get("retard_fin_secondes"),
        "retard_debut_M15_secondes": m15.get("retard_debut_secondes"),
        "retard_fin_M15_secondes": m15.get("retard_fin_secondes"),
        "integralite_180j_prouvee": False,
        "raison_non_preuve": "Sans calendrier de sessions propre au marché, les trous calendaires ne permettent pas de distinguer fermeture normale et données manquantes; les bornes et la pagination sont rapportées séparément.",
    }


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


def _source_fingerprint():
    try:
        with open(__file__, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except OSError:
        return None


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


def trade_audit_chronological_groups(trades, key_fields, window_start, window_end):
    """Statistiques développement / dernier tiers pour chaque marché, sens ou heure."""
    split_t = int(window_start + (window_end - window_start) * 2 / 3)
    buckets = {}
    for trade in trades:
        key = tuple(trade.get(k) for k in key_fields)
        buckets.setdefault(key, []).append(trade)
    out = []
    for key, group in sorted(buckets.items(), key=lambda item: tuple(str(x) for x in item[0])):
        dev = _closed_segment(group, window_start, split_t)
        holdout = _closed_segment(group, split_t, window_end)
        out.append({**dict(zip(key_fields, key)),
                    "developpement": _stats_adjusted(dev, 0.0),
                    "dernier_tiers_diagnostic": _stats_adjusted(holdout, 0.0)})
    return out


def _quality_validation_by_body(trades, window_start, window_end):
    """Résultats par classe de corps, séparés chronologiquement."""
    split_t = int(window_start + (window_end - window_start) * 2 / 3)
    out = {}
    for cls in ("petit_corps", "corps_moyen", "grand_corps", "inconnu"):
        subset = [t for t in trades if t.get("reentry_body_classe") == cls]
        out[cls] = {
            "developpement": _stats_adjusted(_closed_segment(subset, window_start, split_t), 0.0),
            "hors_echantillon": _stats_adjusted(_closed_segment(subset, split_t, window_end), 0.0),
        }
    return out


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
    BT_HISTORY_DIAGNOSTICS.clear()
    BT.update(running=True, done=False, error=None, results=None, progress="connexion à Deriv...")
    http = None
    ws = None
    try:
        http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, connect=15))
        ws = await connect_public_ws(http, attempts=6)
        rows = {(m, t): [] for m in BT_MODES for t in BT_THRS}
        alltr = {(m, t): [] for m in BT_MODES for t in BT_THRS}
        funnel = {m: {"steps": 0, "bias": 0, "zone": 0, "crt": 0, "fvg": 0, "rr": 0,
                      "waiting_confirmation": 0, "confirmed": 0, "rr_ge2": 0,
                      "rr_uncomputable": 0, "zones_etats_final": {"candidates_uniques": 0, "en_attente": 0, "confirmees": 0, "rejetees_tp_risque": 0, "invalidees": 0}, "no_crt_steps": 0, "no_fvg_steps": 0,
                      "body_medium_excluded": 0, "h4_buy": 0, "h4_sell": 0,
                      "direction_blocked": 0, "direction_blocked_buy": 0,
                      "direction_blocked_sell": 0} for m in BT_MODES}
        funnel_by_market = {}
        direction_ab = {}
        split_t = 0
        # Fenêtre UTC figée une seule fois au lancement : aucune donnée future ne peut entrer.
        window_end = int(time.time() // 900) * 900
        window_start = window_end - BT_DAYS * 86400
        n_ok = 0
        coverage = {}
        history_export = {}
        processed_deriv_symbols = {}
        symbol_resolution = {}
        for n, sym in enumerate(SYMBOLS):
            tag = "%d/%d %s" % (n + 1, len(SYMBOLS), sym)
            BT["progress"] = tag + " : téléchargement des bougies..."
            err = None
            actual_sym = real(sym)
            symbol_resolution[sym] = {"symbole_demande": sym, "symbole_deriv_effectif": actual_sym,
                                      "nom_deriv": NAMES.get(actual_sym, actual_sym),
                                      "resolu_par_alias": actual_sym != sym}
            # Deux alias pointant vers le même instrument ne doivent jamais créer
            # deux historiques/trades et gonfler artificiellement le portefeuille.
            if actual_sym in processed_deriv_symbols:
                first = processed_deriv_symbols[actual_sym]
                err = "instrument Deriv dupliqué : %s pointe déjà vers %s via %s" % (sym, actual_sym, first)
                coverage[sym] = {"symbole_deriv_effectif": actual_sym, "exclu_doublon": True,
                                 "doublon_de": first, "preuve_fenetre": {"integralite_180j_prouvee": False}}
                for k in rows:
                    rows[k].append({"symbole": label(sym), "erreur": err})
                continue
            processed_deriv_symbols[actual_sym] = sym
            try:
                h4_raw = await fetch_history(ws, actual_sym, 14400, 1500, 1500, diagnostic_key=sym)
                m15_raw = await fetch_history(ws, actual_sym, 900, (BT_DAYS + 4) * 96, 4000, diagnostic_key=sym)
                # Toutes les unités sont arrêtées à LA MÊME borne fixe du backtest.
                # Une bougie est admissible uniquement si sa clôture est <= window_end.
                d1 = []  # D1 retiré du chemin stratégique : aucune règle ne le consulte.
                h4 = [c for c in h4_raw if int(c["t"]) + 14400 <= window_end]
                m15 = [c for c in m15_raw if int(c["t"]) + 900 <= window_end]
                # L'export n'est ajouté qu'après validation de l'éligibilité du marché
                # et de la présence d'une première bougie M15 dans la fenêtre.
                coverage[sym] = {
                    "D1": {"telecharge": False, "raison": "non utilisé par la stratégie ATLAS; biais et TP H4"},
                    # Couverture calculée sur les mêmes bougies clôturées que replay(),
                    # jamais sur h4_raw qui peut encore contenir la bougie H4 en formation.
                    "H4": history_coverage(h4, 14400, window_start, window_end),
                    "M15": history_coverage(m15, 900, window_start, window_end),
                    "pagination_Deriv": {
                        "H4": BT_HISTORY_DIAGNOSTICS.get((sym, 14400), {}),
                        "M15": BT_HISTORY_DIAGNOSTICS.get((sym, 900), {}),
                    },
                    "H4_warmup_avant_debut": sum(1 for c in h4 if int(c["t"]) < window_start),
                    "M15_warmup_avant_debut": sum(1 for c in m15 if int(c["t"]) < window_start),
                    "regle_couverture": "Les comptes et dates H4/M15 portent sur les bougies clôturées effectivement admissibles; le pourcentage calendaire reste un plafond indicatif car les fermetures de marché ne sont pas retranchées.",
                }
                coverage[sym]["symbole_deriv_effectif"] = actual_sym
                coverage[sym]["nom_deriv"] = NAMES.get(actual_sym, actual_sym)
                coverage[sym]["resolu_par_alias"] = actual_sym != sym
                coverage[sym]["preuve_fenetre"] = classify_history_window(
                    coverage[sym],
                    BT_HISTORY_DIAGNOSTICS.get((sym, 14400), {}),
                    BT_HISTORY_DIAGNOSTICS.get((sym, 900), {}),
                )
                if len(m15) < 400:
                    err = "pas assez d'historique M15 clôturé avant la borne fixe (%d bougies)" % len(m15)
                elif not BT_HISTORY_DIAGNOSTICS.get((sym, 14400), {}).get("complete", False):
                    coverage[sym]["avertissement_pagination_H4"] = "objectif de téléchargement H4 non atteint; couverture à contrôler"
                elif not BT_HISTORY_DIAGNOSTICS.get((sym, 900), {}).get("complete", False):
                    coverage[sym]["avertissement_pagination_M15"] = "objectif de téléchargement M15 non atteint; couverture à contrôler"
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
            # Exactement les historiques des marchés qui entrent effectivement dans replay().
            history_export[sym] = pack_history_export(h4, m15)
            funnel_by_market[sym] = {}
            for mode in BT_MODES:
                BT["progress"] = tag + " : analyse (" + mode + ")..."
                trs, st = await replay(d1, h4, m15, start_i, SIDE_ONLY.get(sym), mode, BT_THRS)
                funnel_by_market[sym][mode] = {k: (len(v) if isinstance(v, set) else v)
                                               for k, v in st.items() if k != "zone_states"}
                funnel_by_market[sym][mode]["zones_etats_final"] = {
                    "candidates_uniques": len(st["zone_states"]),
                    "en_attente": sum(v == "pending" for v in st["zone_states"].values()),
                    "confirmees": sum(v == "confirmed" for v in st["zone_states"].values()),
                    "rejetees_tp_risque": sum(v == "rejected_tp_risk" for v in st["zone_states"].values()),
                    "invalidees": sum(v == "invalidated" for v in st["zone_states"].values()),
                }
                if mode == "fvg" and sym in SIDE_ONLY:
                    # Diagnostic A/B sans restriction : aucune modification du scanner live.
                    free_trs, free_st = await replay(d1, h4, m15, start_i, None, mode, (0.0,))
                    direction_ab[sym] = {
                        "restriction_live_conservee": SIDE_ONLY[sym],
                        "avec_restriction": summarize(sym, trs[0.0]),
                        "sans_restriction_diagnostic": summarize(sym, free_trs[0.0]),
                        "signaux_bloques_direction": st["direction_blocked"],
                        "note": "Comparaison historique diagnostique uniquement; aucune modification de SIDE_ONLY en production."
                    }
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
                f["waiting_confirmation"] += sum(v == "pending" for v in st["zone_states"].values())
                f["confirmed"] += len(st["confirmed"])
                f["rr_ge2"] += len(st["rr_ge2"])
                f["rr_uncomputable"] += len(st["rr_uncomputable"])
                f["zones_etats_final"]["candidates_uniques"] += len(st["zone_states"])
                f["zones_etats_final"]["en_attente"] += sum(v == "pending" for v in st["zone_states"].values())
                f["zones_etats_final"]["confirmees"] += sum(v == "confirmed" for v in st["zone_states"].values())
                f["zones_etats_final"]["rejetees_tp_risque"] += sum(v == "rejected_tp_risk" for v in st["zone_states"].values())
                f["zones_etats_final"]["invalidees"] += sum(v == "invalidated" for v in st["zone_states"].values())
                f["no_crt_steps"] += st["no_crt_steps"]
                f["no_fvg_steps"] += st["no_fvg_steps"]
                f["body_medium_excluded"] += len(st["body_medium_excluded"])
                for counter in ("h4_buy", "h4_sell", "direction_blocked",
                                "direction_blocked_buy", "direction_blocked_sell"):
                    f[counter] += st[counter]
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
                                                  "ecart_resultats_vs_setups": len(combo_trades) - outcome_sum,
                                                  "integrite_portefeuille": portfolio_reconciliation(combo_trades, combo_rows)},
                               "lignes": combo_rows})
        primary_audit = alltr[("fvg", 0.0)]
        # fvg_raw était un alias exact du chemin FVG; une seule simulation est conservée.
        close_audit = []  # Ancien mode CLOSE retiré : non conforme à la stratégie ATLAS
        enrich_trade_quality(primary_audit)
        enrich_trade_quality(close_audit)
        losses_audit = [t for t in primary_audit if t.get("res") == "loss"]
        close_losses_audit = []
        coverage_summary = {
            "marches_analyses": n_ok,
            "marches_exportes": len(history_export),
            "pagination_H4_objectif_atteint": sum(bool(v.get("preuve_fenetre", {}).get("pagination_H4_objectif_atteint")) for v in coverage.values() if "preuve_fenetre" in v),
            "pagination_M15_objectif_atteint": sum(bool(v.get("preuve_fenetre", {}).get("pagination_M15_objectif_atteint")) for v in coverage.values() if "preuve_fenetre" in v),
            "integralite_180j_prouvee": 0,
            "note": "Aucun marché n'est déclaré intégralement couvert sur 180 jours sans calendrier de sessions vérifié; bornes observées et pagination sont exposées séparément.",
        }
        run_id = str(uuid.uuid4())
        code_sha256 = _source_fingerprint()
        BT["results"] = {"run_id": run_id, "code_sha256": code_sha256,
                         "build_id": os.environ.get("RAILWAY_GIT_COMMIT_SHA") or code_sha256,
                         "version": "ADONIS-ATLAS-AUDIT-CONSOLIDE-LOCAL",
                         "crt_h4_contexte_backtest": "tout l’historique H4 disponible avant la bougie M15",
                          "crt_h4_contexte_scanner": "1500 bougies H4 initiales puis fusion cumulative des rafraîchissements pendant la vie du processus; réinitialisé au redémarrage Railway",
                         "trailing_fvg_multi_zones": True,
                         "trailing_priorite": "FVG la plus récente d’abord; anciennes zones conservées comme candidates IFVG",
                         "entree_fvg_ifvg_priorite": "arbitrage chronologique commun; FVG activée à sa création, IFVG activée à la clôture de flip; une zone active plus récente bloque les confirmations des zones plus anciennes",
                         "export_ohlc": {"present": True, "schema": "atlas-ohlc-v1",
                                         "encoding": "gzip+base64 JSON par marché",
                                         "description": "Bougies H4/M15 clôturées réellement utilisées par replay(), warmup inclus; D1 non utilisé.",
                                         "marches_exportes": len(history_export)},
                         "historique_replay_compresse": history_export,
                         "rapport_autonome": True, "comparaison_anciens_json": False,
                         "comparaison_baseline_note": "Aucun ancien JSON n’est comparé automatiquement dans ce run; la référence doit être fournie et fingerprintée.",
                         "scanner_mode": "scanner d alertes, ne suit pas les positions exécutées par le trader; aucun blocage de positions réelles n’est revendiqué",
                         "politique_directionnelle": {
                             "regle_generale": "Le sens du signal est déterminé par structure_bias H4; BUY si H4 up, SELL si H4 down; aucune direction si H4 indéterminé.",
                             "restrictions_par_symbole": dict(SIDE_ONLY),
                             "restriction_active": bool(SIDE_ONLY),
                             "boom_crash_forces": False,
                             "coherence_scanner_backtest": "analyze_core applique la même structure_bias H4 et side_allowed dans le scanner et le replay; le replay vérifie aussi le sens avant simulation.",
                         },
                         "dedup_persistante": False, "dedup_scope": "mémoire du processus; redémarrage Railway réinitialise les clés",
                         "couts_reels_verifies": False,
                         "crt_invalidation_atr_h4": 0.10, "strategie_reference": "ADONIS ATLAS: biais de tendance H4; CRT AMD persistant jusqu’à invalidation par clôture H4 au-delà de 0,10 ATR; FVG M15 prioritaire le plus récent; entrée à la clôture de reclaim après test; SL sous/sur mèche de la bougie de test M15 avec buffer ATR; TP liquidité H4; trailing uniquement après clôture M15 au-delà de la borne opposée CRT, puis nouveau FVG, retest et confirmation; aucun filtre Premium/Discount ni taille moyenne H4",
                         "jours": BT_DAYS, "jours_new": BT_DAYS // 3,
                         "fenetre_debut_utc": window_start, "fenetre_fin_utc": window_end,
                         "bougies_m15_incompletes_exclues": True, "borne_commune_toutes_unites": True,
                         "causalite_h4_m15": {"active": True,
                             "regle": "À chaque clôture M15 T, seules les H4 dont ouverture + 14400 <= T sont visibles; une H4 clôturant exactement à T est admissible.",
                             "controle_runtime": "préfixe H4 strictement chronologique + garde anti-lookahead à chaque étape replay"},
                         "simulateur_sorties": "SL/TP actifs testés avant trailing; nouveau SL actif à la bougie suivante; SL prioritaire si SL et TP touchés dans la même M15; sorties breakeven séparées",
                         "marches": n_ok, "marches_exportes_replay": sorted(history_export),
                         "resolution_symboles_deriv": symbol_resolution,
                         "unicite_instruments_deriv": {"instruments_uniques_analyses": len(processed_deriv_symbols), "alias_doublons_exclus": sum(1 for v in coverage.values() if v.get("exclu_doublon")), "regle": "Un code Deriv effectif ne peut être rejoué qu’une seule fois dans le portefeuille, même si plusieurs symboles demandés le désignent."},
                         "couverture_historique": coverage,
                         "synthese_couverture_historique": coverage_summary,
                         "audit_trades_atlas_fvg": primary_audit,
                         "audit_trades_atlas_fvg_sans_filtre_corps_note": "Alias supprimé : les trades sont déjà dans audit_trades_atlas_fvg; aucun second tableau identique n'est exporté.",
                         "audit_trades_atlas_close": close_audit,
                         "audit_pertes_atlas_fvg": losses_audit,
                         "audit_pertes_atlas_close": close_losses_audit,
                         "audit_par_marche": trade_audit_groups(primary_audit, ("sym",)),
                         "audit_par_marche_close": trade_audit_groups(close_audit, ("sym",)),
                         "audit_par_marche_direction": trade_audit_groups(primary_audit, ("sym", "side")),
                         "audit_par_marche_direction_close": trade_audit_groups(close_audit, ("sym", "side")),
                         "audit_par_marche_heure_utc": trade_audit_groups(primary_audit, ("sym", "hour_utc")),
                         "audit_par_heure_utc": trade_audit_groups(primary_audit, ("hour_utc",)),
                         "validation_chronologique_par_marche_sens": trade_audit_chronological_groups(primary_audit, ("sym", "side"), window_start, window_end),
                         "validation_chronologique_par_marche_heure": trade_audit_chronological_groups(primary_audit, ("sym", "hour_utc"), window_start, window_end),
                         "audit_par_biais": trade_audit_groups(primary_audit, ("bias_d1", "bias_h4")),
                         "audit_par_profondeur_sweep": trade_audit_groups(primary_audit, ("sweep_depth_classe",)),
                         "audit_par_qualite_reintegration": trade_audit_groups(primary_audit, ("reentry_body_classe", "reentry_close_classe")),
                         "validation_qualite_corps_fvg": _quality_validation_by_body(primary_audit, window_start, window_end),
                         "filtre_fvg_corps_moyen": {"actif": False,
                                                     "note": "Filtre supprimé : aucune exclusion selon le ratio corps/range de la bougie H4."},
                         "tp_d1_fvg": {"actif": False, "regle_active": "liquidité H4 uniquement"},
                         "calcul_profondeur_sweep": "directionnel : BUY = range_low - sweep_low; SELL = sweep_high - range_high; valeurs négatives ramenées à zéro",
                         "audit_par_qualite_reintegration_close": trade_audit_groups(close_audit, ("reentry_body_classe", "reentry_close_classe")),
                         "mode_validation": "premiers 2/3 développement; dernier tiers hors échantillon chronologique (désormais diagnostic, car les résultats historiques ont déjà guidé le choix du filtre)",
                         "couts_reels_verifies": False,
                          "dedup_alertes": {"mode":"RAM", "durable_apres_redemarrage":False, "limite":2000, "retry_ntfy":True},
                          "scanner_gestion_positions":False,
                          "scanner_m15_historique_bougies":1500,
                          "trailing_confirmation_separee_du_retest":True,
                          "gap_stop_market": "fill ouverture si gap adverse au-delà du SL; sinon prix SL",
                         "couts_configures": {"BT_COST_R": default_cost_r, "BT_COST_R_BY_SYMBOL": cost_by_symbol},
                         "note_couts": "Sensibilités incluses; les coûts réels doivent être calibrés par marché avec spread, commission et slippage observés.",
                         "entonnoir": funnel,
                         "entonnoir_par_marche": funnel_by_market,
                         "diagnostic_directionnel_boom_crash": direction_ab,
                         "ventilation_rr_descriptive_reference_fvg": rr_descriptive_breakdown(primary_audit),
                         "semantique_rr": {"reference": "RR descriptif post-hoc sur les trades de base sans filtre RR",
                                           "combos": "chaque ligne RR >= X est une simulation de portefeuille distincte avec filtre d'entrée et état d'occupation propre; ce n'est pas un simple sous-ensemble statistique"},
                         "fvg_raw": {"statut": "alias_supprime", "raison": "même chemin de signal que fvg dans le code source; aucun test distinct"},
                         "d1": {"utilisation_strategie": False, "usage": "couverture historique diagnostique uniquement; biais et TP H4"},
                         "drawdown": {"ordre": "sorties chronologiques par exit_t; sorties simultanées agrégées avant calcul du drawdown", "streaks": "égalité exit_t départagée par signal_t pour une série reproductible", "limite": "drawdown réalisé en R; ne constitue pas un drawdown mark-to-market tick par tick"},
                         "exposition": {"max_positions_simultanees": "inclut les positions ouvertes jusqu'à leur dernière bougie connue", "max_risque_initial_simultane_r": "somme du risque initial normalisé à 1R par position simultanée; ne remplace pas une mesure mark-to-market", "mode_portefeuille": "les marchés sont simulés indépendamment puis agrégés; aucun plafond global de positions ou de risque n'est appliqué", "interpretation": "le nombre maximal simultané est une mesure de capacité/exposition observée, pas une contrainte d'exécution simulée"},
                         "couts_reels_verifies": False,
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


async def api_bt_download(request):
    _, s = get_session(request)
    if not s:
        return web.json_response({"error": "not_logged_in"}, status=401)
    if not BT.get("done") or not BT.get("results"):
        return web.json_response({"error": "backtest_not_ready"}, status=409)
    payload = json.dumps(BT, ensure_ascii=False, indent=2)
    return web.Response(
        text=payload,
        content_type="application/json",
        headers={
            "Content-Disposition": 'attachment; filename="ATLAS_AUDITEE_CORRIGEE_CONSOLIDEE_BACKTEST.json"',
            "Cache-Control": "no-store",
        },
    )


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
app.router.add_get("/api/backtest/download", api_bt_download)
app.router.add_get("/api/accounts", api_accounts)
app.router.add_post("/api/otp", api_otp)
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)

if __name__ == "__main__":
    print("DERIV_APP_ID défini :", bool(APP_ID))
    print("REDIRECT_URI :", REDIRECT_URI)
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host="0.0.0.0", port=port)
