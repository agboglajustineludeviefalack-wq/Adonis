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
SWING_N = int(os.environ.get("SWING_N", "2"))        # bougies de chaque côté d'un sommet/creux
MIN_RR = float(os.environ.get("MIN_RR", "2.0"))       # gain/risque minimum
EQ_TOL = float(os.environ.get("EQ_TOL", "0.0005"))   # tolérance des Equal Highs/Lows (0.05 %)
SCAN_EVERY = int(os.environ.get("SCAN_EVERY", "60"))  # secondes entre deux analyses
H4_TTL = 240                                          # secondes avant de retélécharger le H4
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
  const f = res.entonnoir;
  const c2 = res.combos.find(c => c.min_rr === 2);
  const head = document.createElement("div");
  head.className = "log-item";
  head.textContent = "Entonnoir (" + res.marches + " marchés, " + res.jours + " jours) : CRT H4 détectés : " + f.crt + " · FVG M15 repérés : " + f.fvg + " · rejetés par gain/risque < 2 : " + f.rr + " · signaux (RR ≥ 2) : " + (c2 ? c2.total.setups : 0);
  list.appendChild(head);
  res.combos.forEach(c => {
    const t = c.total;
    const done = t.gagnes + t.perdus + t.expires;
    const per = done ? (t.r_total / done).toFixed(2) : "0.00";
    const det = document.createElement("details");
    det.className = "log-item";
    det.style.borderLeftColor = t.r_total > 0 ? "#22c55e" : "#ef4444";
    const sm = document.createElement("summary");
    sm.textContent = "CRT H4 + rejet du FVG M15 · RR ≥ " + c.min_rr + " : " + t.setups + " trades · " + t.gagnes + " gagnés · " + t.perdus + " perdus · " + sign(t.r_total) + " R (" + sign(per) + " R/trade) · avant les " + res.jours_new + " derniers jours : " + t.n_old + " trades, " + sign(t.r_old) + " R · " + res.jours_new + " derniers jours : " + t.n_new + " trades, " + sign(t.r_new) + " R";
    det.appendChild(sm);
    c.lignes.forEach(l => {
      const d = document.createElement("div");
      d.style.marginTop = "4px";
      d.textContent = l.erreur ? (l.symbole + " — erreur : " + l.erreur) : (l.symbole + " — " + l.setups + " trades · " + l.gagnes + " G · " + l.perdus + " P · " + sign(l.r_total) + " R (achats " + l.achats + " : " + sign(l.r_achats) + " R, ventes " + l.ventes + " : " + sign(l.r_ventes) + " R)");
      det.appendChild(d);
    });
    list.appendChild(det);
  });
  const note = document.createElement("div");
  note.className = "small";
  note.style.marginTop = "6px";
  note.textContent = "Rappel : sans spread ni commission. Moins de 100 trades par variante = on ne peut rien conclure.";
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
#  ANALYSE : CRT en H4, puis FVG en M15, puis entrée au REJET du FVG.
#  Plus de biais de structure, plus de discount/premium, plus de veto Daily.
#  Les fonctions ci-dessous ne font aucun appel réseau.
#  Chaque bougie = {"t": début en secondes, "o", "h", "l", "c"}
#  Pour les VENTES, on inverse les prix (miroir) et on réutilise
#  exactement la logique des ACHATS, puis on remet les prix à l'endroit.
#
#  La stratégie, étape par étape (version ACHAT) :
#   1. H4 : la bougie 1 crée un range (son haut et son bas).
#   2. H4 : la bougie 2 mèche SOUS le bas de la bougie 1 (sans dépasser son haut)
#      et CLÔTURE à l'intérieur du range. On ne descend en M15 qu'à ce moment-là.
#   3. M15 : le retour dans le range a laissé un FVG haussier (après le point de sweep).
#   4. M15 : quand une bougie touche le FVG et CLÔTURE AU-DESSUS de lui, c'est un rejet :
#      signal d'entrée à la clôture de cette bougie (après la clôture de la bougie 2 H4).
#   5. Stop sous le point de sweep, TP sur la prochaine liquidité, gain/risque minimum.
#  VENTE = exactement l'inverse (sweep du haut, FVG baissier).
# ==================================================================
RANK = {"no_crt": 0, "crt_invalid": 1, "no_fvg": 2, "waiting_retrace": 3, "rr_low": 4, "signal": 5}


def real(sym):
    return RESOLVED.get(sym, sym)


def label(sym):
    return MT5.get(sym, sym)


def allowed(only, sd):
    """Boom = achats seulement, Crash = ventes seulement (sd vaut 'buy' ou 'sell')."""
    return not only or only == sd


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


def pick_tp(entry, h4c, c1_high):
    """Prochaine liquidité au-dessus de l'entrée : Equal Highs, sommet H4 ou haut du CRT (le plus proche)."""
    hs, _ = swings(h4c[-150:], SWING_N)
    levels = [h for _, h in hs if h > entry]
    if c1_high > entry:
        levels.append(c1_high)
    if not levels:
        return None, None
    tp = min(levels)
    near = [h for _, h in hs if abs(h - tp) <= abs(tp) * EQ_TOL]
    if len(near) >= 2:
        label_ = "Equal Highs"
    elif tp == c1_high:
        label_ = "haut du CRT"
    else:
        label_ = "sommet H4"
    return tp, label_


def crt_buy(c1, c2):
    """CRT d'achat en H4 : la bougie 2 mèche SOUS le bas de la bougie 1, ne dépasse pas son haut,
    et clôture à l'intérieur du range de la bougie 1."""
    return c2["l"] < c1["l"] and c2["h"] <= c1["h"] and c1["l"] < c2["c"] <= c1["h"]


def crt_dir(c1, c2):
    """'buy' (sweep du bas), 'sell' (sweep du haut) ou None."""
    if crt_buy(c1, c2):
        return "buy"
    if crt_buy(mirror_one(c1), mirror_one(c2)):
        return "sell"
    return None


def crt_entry(c1, c2, m15c, h4c, min_rr=None):
    """Partie M15 d'un CRT d'achat déjà validé en H4. Toutes les M15 sont CLÔTURÉES."""
    min_rr = MIN_RR if min_rr is None else min_rr
    base = {"c1_t": c1["t"], "c1_high": c1["h"], "c1_low": c1["l"], "c2_t": c2["t"]}
    c2_end = c2["t"] + 14400

    in_c2 = [(i, m) for i, m in enumerate(m15c) if c2["t"] <= m["t"] < c2_end]
    after = [m for m in m15c if m["t"] >= c2_end]
    if not in_c2 or not after:
        return dict(base, code="no_fvg")   # on ne descend en M15 qu'APRÈS la clôture de la bougie 2
    k_low, low_m = min(in_c2, key=lambda x: x[1]["l"])
    sweep_low = min(c2["l"], low_m["l"])
    if min(m["l"] for m in after) < sweep_low:
        return dict(base, code="crt_invalid")   # le prix a repris le point de sweep : le CRT a échoué

    # FVG haussiers formés par le retour dans le range, à partir du point de sweep.
    n = len(m15c)
    cands = []
    for i in range(k_low, n - 3):          # il reste au moins une bougie distincte après le FVG
        a, c = m15c[i], m15c[i + 2]
        if c["l"] > a["h"]:
            bottom, top = a["h"], c["l"]
            if any(m15c[k]["c"] < bottom for k in range(i + 3, n)):
                continue                    # une clôture sous le bas du FVG l'invalide
            cands.append((i, bottom, top))
    if not cands:
        return dict(base, code="no_fvg")

    # Rejet : la dernière M15 clôturée touche le FVG (bas <= haut du FVG) et CLÔTURE AU-DESSUS.
    last = m15c[-1]
    sel = None
    for i, bottom, top in reversed(cands):
        if last["l"] <= top and last["c"] > top:
            sel = (i, bottom, top)
            break
    if sel is None:
        i, bottom, top = cands[-1]
        return dict(base, code="waiting_retrace", fvg_bottom=bottom, fvg_top=top,
                    fvg_t=m15c[i]["t"], sweep_low=sweep_low)

    i, bottom, top = sel
    entry = last["c"]
    out = dict(base, fvg_bottom=bottom, fvg_top=top, fvg_t=m15c[i]["t"],
               sweep_low=sweep_low, retest_t=last["t"])
    if entry <= sweep_low:
        out["code"] = "rr_low"
        return out
    tp, label_ = pick_tp(entry, h4c, c1["h"])
    if tp is None or tp <= entry:
        out["code"] = "rr_low"
        return out
    risk = entry - sweep_low
    rr = (tp - entry) / risk
    out.update(entry=entry, sl=sweep_low, tp=tp, tp_label=label_, rr=rr)
    out["code"] = "signal" if rr >= min_rr else "rr_low"
    return out


def evaluate_buy(h4c, m15c, min_rr=None):
    if len(h4c) < 2 or not m15c:
        return {"code": "no_crt"}
    c1, c2 = h4c[-2], h4c[-1]          # les 2 dernières bougies H4 CLÔTURÉES
    if not crt_buy(c1, c2):
        return {"code": "no_crt"}
    return crt_entry(c1, c2, m15c, h4c, min_rr)


def unmirror(m):
    out = dict(m)
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


def analyze(sym, h4, m15, now):
    """Version en direct : ne garde que les bougies CLÔTURÉES, puis analyse."""
    if m15 and now - (m15[-1]["t"] + 900) > 1800:
        return {"resume": "marché fermé ou données anciennes"}, None
    h4c, _ = split_closed(h4, 14400, now)
    m15c, _ = split_closed(m15, 900, now)
    return analyze_core(h4c, m15c, SIDE_ONLY.get(sym))


def analyze_core(h4c, m15c, only=None, min_rr=None):
    """Retourne (infos_pour_la_page, signal_ou_None)."""
    if len(h4c) < 15 or len(m15c) < 15:
        return {"resume": "pas assez de bougies reçues (H4 %d, M15 %d)" % (len(h4c), len(m15c)), "code": "no_crt"}, None

    px = " · prix " + fmt(m15c[-1]["c"])
    c1, c2 = h4c[-2], h4c[-1]
    sd = crt_dir(c1, c2)
    if sd is None:
        return {"resume": "pas de CRT H4 sur les 2 dernières bougies (la 2e doit mécher UN côté de la 1re et clôturer dedans)" + px,
                "code": "no_crt"}, None
    word = "d'achat (sweep du bas)" if sd == "buy" else "de vente (sweep du haut)"
    if not allowed(only, sd):
        return {"resume": "CRT H4 %s détecté, mais ici on ne trade que les %s (spikes) : pas de trade%s" % (
            word, "achats" if only == "buy" else "ventes", px), "code": "no_crt"}, None

    if sd == "buy":
        res = evaluate_buy(h4c, m15c, min_rr)
    else:
        res = unmirror(evaluate_buy(mirror(h4c), mirror(m15c), min_rr))

    code = res["code"]
    msgs = {
        "no_crt": "pas de CRT H4",
        "crt_invalid": "CRT H4 %s invalidé (le prix a repris le point de sweep)" % word,
        "no_fvg": "CRT H4 %s validé, pas encore de FVG M15 exploitable (ou bougie 3 pas commencée)" % word,
        "waiting_retrace": "CRT H4 %s validé, FVG M15 repéré, attente du rejet du FVG" % word,
        "rr_low": "rejet du FVG vu mais gain/risque trop faible (%s)" % (("%.1f" % res["rr"]) if "rr" in res else "pas de cible"),
        "signal": "SETUP A+ (CRT H4 + rejet du FVG M15)",
    }
    info = {"resume": msgs[code] + px, "code": code,
            "c1_t": res.get("c1_t"), "fvg_t": res.get("fvg_t"), "rr": res.get("rr")}
    if code == "signal":
        sig = dict(res)
        sig["side"] = sd
        return info, sig
    return info, None


def build_text(sym, sig):
    buy = sig["side"] == "buy"
    return "\n".join([
        "%s %s - Setup A+" % ("ACHAT" if buy else "VENTE", label(sym)),
        "CRT H4 : la bougie 2 a mèché le %s de la bougie 1 puis clôturé dans son range" % ("bas" if buy else "haut"),
        "FVG M15 rejeté : %s - %s" % (fmt(sig["fvg_bottom"]), fmt(sig["fvg_top"])),
        "Entrée repère (clôture M15 du rejet) : %s" % fmt(sig["entry"]),
        "Stop : %s (distance %s, soit %.2f%% du prix)" % (fmt(sig["sl"]), fmt(abs(sig["entry"] - sig["sl"])), abs(sig["entry"] - sig["sl"]) / abs(sig["entry"]) * 100),
        "TP : %s (%s, soit %.2f%% du prix)" % (fmt(sig["tp"]), sig["tp_label"], abs(sig["tp"] - sig["entry"]) / abs(sig["entry"]) * 100),
        "Gain/risque : %.1f pour 1" % sig["rr"],
        "Prix Deriv au signal : %s. Compare avec ton MT5 avant d'entrer." % fmt(sig["entry"]),
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
    await send_ntfy("Setup A+ %s %s" % (label(sym), "ACHAT" if sig["side"] == "buy" else "VENTE"), text)


def stale(fetched, now, ttl, gran):
    return now - fetched > ttl or int(now // gran) != int(fetched // gran)


async def scan_once(ws, cache):
    for sym in SYMBOLS:
        try:
            now = time.time()
            hk = (sym, 14400)
            if hk not in cache or stale(cache[hk][0], now, H4_TTL, 14400):
                cache[hk] = (now, await fetch_candles(ws, real(sym), 14400, 300))
            m15 = await fetch_candles(ws, real(sym), 900, 300)
            info, sig = analyze(sym, cache[hk][1], m15, time.time())
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
#  TEST HISTORIQUE : rejoue la stratégie sur le passé, bougie M15 par bougie M15.
#  Règle anti-triche : à chaque instant, l'analyse ne voit que des bougies CLÔTURÉES
#  (H4 et M15) qui existaient réellement à ce moment-là.
# ==================================================================
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


async def replay(h4, m15, start_i, only=None, thresholds=None):
    thresholds = tuple(thresholds or (MIN_RR,))
    lo_rr = min(thresholds)
    h4_t = [c["t"] for c in h4]
    crt_cache = {}
    seen = {t: set() for t in thresholds}
    trades = {t: [] for t in thresholds}
    busy = {t: -1 for t in thresholds}
    st = {"steps": 0, "crt": set(), "fvg": set(), "rr": set()}
    for i in range(start_i, len(m15)):
        if i % 200 == 0:
            await asyncio.sleep(0)  # laisse respirer le serveur
        T = m15[i]["t"] + 900
        kh = bisect_right(h4_t, T - 14400)   # bougies H4 CLÔTURÉES à l'instant T
        if kh < 16:
            continue
        st["steps"] += 1
        sd = crt_cache.get(kh, "?")
        if sd == "?":
            sd = crt_dir(h4[kh - 2], h4[kh - 1])
            crt_cache[kh] = sd
        if sd is None or not allowed(only, sd):
            continue
        free = [t for t in thresholds if i > busy[t]]
        if not free:
            continue
        h4c = h4[max(0, kh - 300):kh]
        m15c = m15[max(0, i + 1 - 300):i + 1]
        info, sig = analyze_core(h4c, m15c, only, lo_rr)
        rk = RANK.get(info.get("code"), -1)
        if rk >= RANK["crt_invalid"] and info.get("c1_t"):
            st["crt"].add(info["c1_t"])
        k2 = (info.get("c1_t"), info.get("fvg_t"))
        if rk >= RANK["waiting_retrace"] and k2[0] and k2[1]:
            st["fvg"].add(k2)
        if info.get("code") in ("rr_low", "signal") and k2[0] and k2[1]:
            rr_v = sig["rr"] if sig else info.get("rr")
            if rr_v is None or rr_v < MIN_RR:
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
                trades[thr].append({"side": sig["side"], "t": T, "res": res, "r": r, "rr": sig["rr"]})
                busy[thr] = j
    return trades, st


def summarize(sym, trades):
    w = sum(1 for t in trades if t["res"] == "win")
    l = sum(1 for t in trades if t["res"] == "loss")
    to = sum(1 for t in trades if t["res"] == "timeout")
    op = sum(1 for t in trades if t["res"] == "open")
    total_r = sum(t["r"] for t in trades if t["res"] != "open")
    buys = [t for t in trades if t["side"] == "buy" and t["res"] != "open"]
    sells = [t for t in trades if t["side"] == "sell" and t["res"] != "open"]
    return {"symbole": label(sym), "achats": len(buys), "r_achats": round(sum(t["r"] for t in buys), 2),
            "ventes": len(sells), "r_ventes": round(sum(t["r"] for t in sells), 2), "setups": len(trades),
            "gagnes": w, "perdus": l, "expires": to, "ouverts": op, "r_total": round(total_r, 2)}


def combo_total(trades, split_t):
    closed = [t for t in trades if t["res"] != "open"]
    old = [t for t in closed if t["t"] < split_t]
    new = [t for t in closed if t["t"] >= split_t]
    return {"setups": len(trades),
            "gagnes": sum(1 for t in trades if t["res"] == "win"),
            "perdus": sum(1 for t in trades if t["res"] == "loss"),
            "expires": sum(1 for t in trades if t["res"] == "timeout"),
            "ouverts": sum(1 for t in trades if t["res"] == "open"),
            "r_total": round(sum(t["r"] for t in closed), 2),
            "n_old": len(old), "r_old": round(sum(t["r"] for t in old), 2),
            "n_new": len(new), "r_new": round(sum(t["r"] for t in new), 2)}


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


BT_THRS = (1.0, 1.5, 2.0)


async def run_backtest():
    BT.update(running=True, done=False, error=None, results=None, progress="connexion à Deriv...")
    http = None
    ws = None
    try:
        http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, connect=15))
        ws = await http.ws_connect(PUBLIC_WS, heartbeat=25)
        rows = {t: [] for t in BT_THRS}
        alltr = {t: [] for t in BT_THRS}
        funnel = {"steps": 0, "crt": 0, "fvg": 0, "rr": 0}
        window_end = int(time.time() // 900) * 900
        start_t = window_end - BT_DAYS * 86400
        split_t = window_end - (BT_DAYS // 3) * 86400
        n_ok = 0
        for n, sym in enumerate(SYMBOLS):
            tag = "%d/%d %s" % (n + 1, len(SYMBOLS), sym)
            BT["progress"] = tag + " : téléchargement des bougies..."
            err = None
            try:
                h4 = await fetch_history(ws, real(sym), 14400, (BT_DAYS + 30) * 6, 4000)
                m15 = await fetch_history(ws, real(sym), 900, (BT_DAYS + 4) * 96, 4000)
                # Ne jamais traiter la bougie M15 encore en formation comme clôturée.
                m15 = [c for c in m15 if int(c["t"]) + 900 <= int(time.time())]
                if len(m15) < 400:
                    err = "pas assez d'historique (%d bougies M15)" % len(m15)
            except ApiError as e:
                err = str(e)
            if err:
                for k in rows:
                    rows[k].append({"symbole": label(sym), "erreur": err})
                continue
            n_ok += 1
            start_i = next((i for i, c in enumerate(m15) if c["t"] >= start_t), 0)
            BT["progress"] = tag + " : analyse..."
            trs, st = await replay(h4, m15, start_i, SIDE_ONLY.get(sym), BT_THRS)
            for thr in BT_THRS:
                rows[thr].append(summarize(sym, trs[thr]))
                alltr[thr].extend(trs[thr])
            funnel["steps"] += st["steps"]
            funnel["crt"] += len(st["crt"])
            funnel["fvg"] += len(st["fvg"])
            funnel["rr"] += len(st["rr"])
        combos = [{"min_rr": thr, "total": combo_total(alltr[thr], split_t), "lignes": rows[thr]}
                  for thr in BT_THRS]
        BT["results"] = {"jours": BT_DAYS, "jours_new": BT_DAYS // 3, "marches": n_ok,
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
