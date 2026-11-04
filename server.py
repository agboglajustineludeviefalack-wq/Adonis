import os
import re
import json
import time
import html
import base64
import hashlib
import secrets
from urllib.parse import urlencode

import aiohttp
from aiohttp import web

# ------------------------------------------------------------------
# CONFIGURATION (variables à mettre dans Railway > Variables)
#   DERIV_APP_ID : l'ID de ton application sur developers.deriv.com
#   PUBLIC_URL   : (optionnel) l'adresse publique de ton site Railway
# ------------------------------------------------------------------
APP_ID = os.environ.get("DERIV_APP_ID", "").strip()
PUBLIC_URL = os.environ.get(
    "PUBLIC_URL", "https://adonis-production-8b23.up.railway.app"
).rstrip("/")
REDIRECT_URI = PUBLIC_URL + "/callback"

AUTH_URL = "https://auth.deriv.com/oauth2/auth"
TOKEN_URL = "https://auth.deriv.com/oauth2/token"
API_BASE = "https://api.derivws.com"

TIMEOUT = aiohttp.ClientTimeout(total=20)
ACCOUNT_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

# Mémoire du serveur (se vide si Railway redémarre : il faudra se reconnecter)
SESSIONS = {}  # sid -> {"token": ..., "exp": ...}
PENDING = {}   # state -> {"verifier": ..., "ts": ...}


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
    <p class="small">Aucune analyse n'est codée pour l'instant : cette zone affiche seulement le prix. Le setup A+ viendra quand on aura écrit tes règles.</p>
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
app.router.add_get("/api/accounts", api_accounts)
app.router.add_post("/api/otp", api_otp)

if __name__ == "__main__":
    print("DERIV_APP_ID défini :", bool(APP_ID))
    print("REDIRECT_URI :", REDIRECT_URI)
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host="0.0.0.0", port=port)
