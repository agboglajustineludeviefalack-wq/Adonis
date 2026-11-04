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
SYMBOLS = [
    s.strip()
    for s in os.environ.get(
        "SCAN_SYMBOLS",
        "1HZ50V,R_50,stpRNG,BOOM500,CRASH500,"
        "frxXAUUSD,frxXAGUSD,"
        "frxEURUSD,frxGBPUSD,frxUSDJPY,frxAUDUSD,frxUSDCAD,frxUSDCHF,frxNZDUSD,frxEURGBP,frxEURJPY,frxGBPJPY,"
        "cryBTCUSD,cryETHUSD"
    ).split(",")
    if s.strip()
]
SWING_N = int(os.environ.get("SWING_N", "2"))        # bougies de chaque côté d'un sommet/creux
MIN_RR = float(os.environ.get("MIN_RR", "2.0"))       # gain/risque minimum
EQ_TOL = float(os.environ.get("EQ_TOL", "0.0005"))   # tolérance des Equal Highs/Lows (0.05 %)
SCAN_EVERY = int(os.environ.get("SCAN_EVERY", "60"))  # secondes entre deux analyses
H4_TTL = 240                                          # secondes avant de retélécharger le H4
BT_DAYS = int(os.environ.get("BT_DAYS", "90"))        # durée du test historique (jours)

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
<title>Adonis - Terminal Pro</title>
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
    <p class="small">Rejoue tes règles sur le passé, marché par marché, sans spread ni commission. Résultats en « R » : 1 R = ce que tu risques sur un trade (perdu = -1 R, gagné = +gain/risque).</p>
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
    <p class="small">Cette zone affiche seulement le prix. L'analyse A+ est faite par le scanner du serveur (carte ci-dessus).</p>
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
  $("scanMeta").textContent = "Dernière analyse : " + when + " · alertes ntfy : " + (j.ntfy ? "actives" : "NON configurées") + (j.erreur ? " · erreur : " + j.erreur : "");
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
    d.textContent = sym + " — " + j.symboles[sym].resume;
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
  const t = j.results.total;
  const head = document.createElement("div");
  head.className = "log-item";
  head.style.borderLeftColor = "#22c55e";
  head.textContent = "TOTAL (" + j.results.jours + " jours) : " + t.setups + " setups · " + t.gagnes + " gagnés · " + t.perdus + " perdus · " + t.expires + " sans issue · " + (t.r_total >= 0 ? "+" : "") + t.r_total + " R" + (t.setups < 30 ? " — échantillon trop petit pour conclure" : "");
  list.appendChild(head);
  j.results.lignes.forEach(l => {
    const d = document.createElement("div");
    d.className = "log-item";
    d.textContent = l.erreur ? (l.symbole + " — erreur : " + l.erreur) : (l.symbole + " — " + l.setups + " setups · " + l.gagnes + " gagnés · " + l.perdus + " perdus · " + l.expires + " sans issue · " + (l.r_total >= 0 ? "+" : "") + l.r_total + " R");
    list.appendChild(d);
  });
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
}
ARROW = {"up": "↑", "down": "↓", None: "?"}


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


def pick_tp(entry, h4c, c1_high):
    hs, _ = swings(h4c[-150:], SWING_N)
    levels = [h for _, h in hs if h > entry]
    if c1_high > entry:
        levels.append(c1_high)
    if not levels:
        return None, None
    tp = min(levels)
    near = [h for _, h in hs if abs(h - tp) <= abs(tp) * EQ_TOL]
    if len(near) >= 2:
        label = "Equal Highs"
    elif tp == c1_high:
        label = "haut du CRT"
    else:
        label = "sommet H4"
    return tp, label


def find_entry(c1, c2, m15c, allm, h4c):
    win = [(i, m) for i, m in enumerate(allm) if m["t"] >= c2["t"]]
    s = next((i for i, m in win if m["l"] < c1["l"]), None)
    if s is None:
        return {"code": "no_sweep"}
    j = next((i for i in range(s, len(m15c)) if m15c[i]["c"] > c1["l"]), None)
    if j is None:
        return {"code": "no_reentry"}
    sweep_low = min(m["l"] for _, m in win)

    cands = []
    for i in range(s, len(m15c) - 2):
        a, c = m15c[i], m15c[i + 2]
        if i + 2 >= j and c["l"] > a["h"]:
            bottom, top = a["h"], c["l"]
            if all(m15c[k]["c"] >= bottom for k in range(i + 3, len(m15c))):
                cands.append((i, bottom, top))
    if not cands:
        return {"code": "no_fvg"}
    i, bottom, top = cands[-1]
    price = allm[-1]["c"]
    if price < bottom:
        return {"code": "no_fvg"}

    out = {
        "fvg_bottom": bottom, "fvg_top": top, "fvg_t": m15c[i]["t"],
        "sweep_low": sweep_low, "c1_t": c1["t"], "c1_high": c1["h"], "c1_low": c1["l"],
    }
    touch = [k for k in range(i + 3, len(allm)) if allm[k]["l"] <= top]
    if not touch or touch[-1] < len(allm) - 3:
        out["code"] = "waiting_retrace"
        return out

    entry = top
    if entry <= sweep_low:
        out["code"] = "rr_low"
        return out
    tp, label = pick_tp(entry, h4c, c1["h"])
    if tp is None:
        out["code"] = "rr_low"
        return out
    rr = (tp - entry) / (entry - sweep_low)
    out.update(entry=entry, sl=sweep_low, tp=tp, tp_label=label, rr=rr)
    out["code"] = "signal" if rr >= MIN_RR else "rr_low"
    return out


def evaluate_buy(d1c, h4c, h4f, m15c, m15f):
    allm = m15c + ([m15f] if m15f else [])
    price = allm[-1]["c"]
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
        res = dict(base, **find_entry(c1, c2, m15c, allm, h4c))
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
    return analyze_core(d1c, h4c, h4f, m15c, m15f)


def analyze_core(d1c, h4c, h4f, m15c, m15f, biases=None):
    """Retourne (infos_pour_la_page, signal_ou_None)."""
    if len(d1c) < 15 or len(h4c) < 15 or len(m15c) < 15:
        return {"resume": "pas assez de bougies reçues (D1 %d, H4 %d, M15 %d)" % (len(d1c), len(h4c), len(m15c))}, None

    if biases:
        b_d1, b_h4 = biases
    else:
        b_d1, b_h4 = structure_bias(d1c, SWING_N), structure_bias(h4c, SWING_N)
    head = "D1 %s · H4 %s" % (ARROW[b_d1], ARROW[b_h4])
    if b_d1 is None or b_d1 != b_h4:
        return {"resume": head + " · pas de biais commun, pas de trade"}, None

    buy = (b_d1 == "up")
    zone = "discount" if buy else "premium"
    if buy:
        res = evaluate_buy(d1c, h4c, h4f, m15c, m15f)
    else:
        res = unmirror(evaluate_buy(mirror(d1c), mirror(h4c), mirror_one(h4f), mirror(m15c), mirror_one(m15f)))

    code = res["code"]
    if RANK[code] >= RANK["no_crt"]:
        head += " · en " + zone
    msgs = {
        "no_range": "range Daily introuvable",
        "not_in_zone": "prix pas en %s (prix %s, équilibre %s)" % (zone, fmt(res.get("price", 0)), fmt(res.get("eq", 0))),
        "no_crt": "pas de CRT H4 en cours",
        "no_sweep": "CRT H4 vu, sweep pas encore visible en M15",
        "no_reentry": "sweep fait, pas encore de clôture M15 dans le range",
        "no_fvg": "retour dans le range, pas de FVG M15 valide",
        "waiting_retrace": "FVG M15 repéré, attente du retour du prix dessus",
        "rr_low": "setup trouvé mais gain/risque trop faible (%s)" % (("%.1f" % res["rr"]) if "rr" in res else "pas de cible"),
        "signal": "SETUP A+",
    }
    info = {"resume": head + " · " + msgs[code]}
    if code == "signal":
        sig = dict(res)
        sig["side"] = "buy" if buy else "sell"
        return info, sig
    return info, None


def build_text(sym, sig):
    buy = sig["side"] == "buy"
    return "\n".join([
        "%s %s - Setup A+" % ("ACHAT" if buy else "VENTE", sym),
        "Biais Daily + H4 %s, prix en %s" % ("haussier" if buy else "baissier", "discount" if buy else "premium"),
        "CRT H4 : sweep puis retour dans le range",
        "Zone FVG M15 : %s - %s" % (fmt(sig["fvg_bottom"]), fmt(sig["fvg_top"])),
        "Entrée repère : %s" % fmt(sig["entry"]),
        "Stop : %s (distance %s, soit %.2f%% du prix)" % (fmt(sig["sl"]), fmt(abs(sig["entry"] - sig["sl"])), abs(sig["entry"] - sig["sl"]) / abs(sig["entry"]) * 100),
        "TP : %s (%s)" % (fmt(sig["tp"]), sig["tp_label"]),
        "Gain/risque : %.1f pour 1" % sig["rr"],
        "Aucun ordre n'est passé : vérifie sur le graphique avant d'entrer.",
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
    for sym in SYMBOLS:
        if items and sym not in ids:
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
    await send_ntfy("Setup A+ %s %s" % (sym, "ACHAT" if sig["side"] == "buy" else "VENTE"), text)


def stale(fetched, now, ttl, gran):
    return now - fetched > ttl or int(now // gran) != int(fetched // gran)


async def scan_once(ws, cache):
    for sym in SYMBOLS:
        try:
            now = time.time()
            ck, hk = (sym, 86400), (sym, 14400)
            if ck not in cache or stale(cache[ck][0], now, 900, 86400):
                cache[ck] = (now, await fetch_candles(ws, sym, 86400, 150))
            if hk not in cache or stale(cache[hk][0], now, H4_TTL, 14400):
                cache[hk] = (now, await fetch_candles(ws, sym, 14400, 300))
            m15 = await fetch_candles(ws, sym, 900, 300)
            info, sig = analyze(sym, cache[ck][1], cache[hk][1], m15, time.time())
        except ApiError as e:
            if "rate" in str(e).lower():
                STATE["error"] = "limite de requêtes Deriv atteinte, nouvel essai au prochain tour"
                return
            info, sig = {"resume": "erreur Deriv : %s" % e}, None
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
    """Suit le trade après le signal. Si stop et TP sont touchés dans la même bougie : stop (pessimiste)."""
    buy = sig["side"] == "buy"
    entry, sl, tp = sig["entry"], sig["sl"], sig["tp"]
    risk = abs(entry - sl)
    last_j = min(len(m15) - 1, i + max_bars)
    for j in range(i + 1, last_j + 1):
        c = m15[j]
        hit_sl = (c["l"] <= sl) if buy else (c["h"] >= sl)
        hit_tp = (c["h"] >= tp) if buy else (c["l"] <= tp)
        if hit_sl:
            return "loss", -1.0, j
        if hit_tp:
            return "win", sig["rr"], j
    close = m15[last_j]["c"]
    r = ((close - entry) if buy else (entry - close)) / risk
    return ("timeout" if i + max_bars < len(m15) else "open"), r, last_j


async def replay(d1, h4, m15, start_i):
    d1_t = [c["t"] for c in d1]
    h4_t = [c["t"] for c in h4]
    cache = {}
    seen = set()
    trades = []
    busy = -1
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
        if b_d1 is None or b_d1 != b_h4 or i <= busy:
            continue
        d1c = d1[max(0, kd - 150):kd]
        h4c = h4[max(0, kh - 300):kh]
        m15c = m15[max(0, i + 1 - 300):i + 1]
        h4f = partial_candle(m15, i, T, 14400)
        _, sig = analyze_core(d1c, h4c, h4f, m15c, None, (b_d1, b_h4))
        if not sig:
            continue
        k = (sig["side"], sig["c1_t"], sig["fvg_t"])
        if k in seen:
            continue
        seen.add(k)
        res, r, j = simulate(sig, m15, i)
        trades.append({"side": sig["side"], "t": T, "res": res, "r": r, "rr": sig["rr"]})
        busy = j
    return trades


def summarize(sym, trades):
    w = sum(1 for t in trades if t["res"] == "win")
    l = sum(1 for t in trades if t["res"] == "loss")
    to = sum(1 for t in trades if t["res"] == "timeout")
    op = sum(1 for t in trades if t["res"] == "open")
    total_r = sum(t["r"] for t in trades if t["res"] != "open")
    return {"symbole": sym, "setups": len(trades), "gagnes": w, "perdus": l,
            "expires": to, "ouverts": op, "r_total": round(total_r, 2)}


async def fetch_history(ws, sym, gran, target, page):
    got = {}
    end = "latest"
    for _ in range(8):
        data = await ws_request(ws, {"ticks_history": sym, "end": end, "style": "candles",
                                     "granularity": gran, "count": page}, timeout=40)
        cs = norm_candles(data.get("candles"))
        fresh = [c for c in cs if c["t"] not in got]
        for c in cs:
            got[c["t"]] = c
        if not cs or len(got) >= target or len(fresh) <= 1:
            break
        end = str(cs[0]["t"])
        await asyncio.sleep(0.3)
    return sorted(got.values(), key=lambda c: c["t"])


async def run_backtest():
    BT.update(running=True, done=False, error=None, results=None, progress="connexion à Deriv...")
    http = None
    ws = None
    try:
        http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, connect=15))
        ws = await http.ws_connect(PUBLIC_WS, heartbeat=25)
        rows = []
        for n, sym in enumerate(SYMBOLS):
            tag = "%d/%d %s" % (n + 1, len(SYMBOLS), sym)
            BT["progress"] = tag + " : téléchargement des bougies..."
            try:
                d1 = await fetch_history(ws, sym, 86400, 400, 400)
                h4 = await fetch_history(ws, sym, 14400, 1500, 1500)
                m15 = await fetch_history(ws, sym, 900, (BT_DAYS + 4) * 96, 4000)
            except ApiError as e:
                rows.append({"symbole": sym, "erreur": str(e)})
                continue
            if len(m15) < 400:
                rows.append({"symbole": sym, "erreur": "pas assez d'historique (%d bougies M15)" % len(m15)})
                continue
            BT["progress"] = tag + " : analyse..."
            start_t = m15[-1]["t"] - BT_DAYS * 86400
            start_i = next((i for i, c in enumerate(m15) if c["t"] >= start_t), 0)
            trades = await replay(d1, h4, m15, start_i)
            rows.append(summarize(sym, trades))
        ok = [r for r in rows if "erreur" not in r]
        total = {
            "setups": sum(r["setups"] for r in ok), "gagnes": sum(r["gagnes"] for r in ok),
            "perdus": sum(r["perdus"] for r in ok), "expires": sum(r["expires"] for r in ok),
            "ouverts": sum(r["ouverts"] for r in ok), "r_total": round(sum(r["r_total"] for r in ok), 2),
        }
        BT["results"] = {"jours": BT_DAYS, "lignes": rows, "total": total}
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


async def api_scanner(request):
    _, s = get_session(request)
    if not s:
        return web.json_response({"error": "not_logged_in"}, status=401)
    return web.json_response({
        "derniere_analyse": STATE["last"],
        "erreur": STATE["error"],
        "ntfy": bool(NTFY_TOPIC),
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
