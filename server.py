import os, json, time, math, asyncio, re, base64, hashlib, secrets, html
from urllib.parse import urlencode
from bisect import bisect_left
from collections import deque
from datetime import datetime, timezone
import aiohttp
from aiohttp import web

# ================================================================
# ADONIS — CRT H4 + retest FVG/OB M15
# Conservés : application, OAuth Deriv, compte, flux prix, scanner, Notify, backtest, JSON.
# Moteur (même logique que l'EA MT5) :
#   1. Tendance H4 : sommets ET creux de plus en plus hauts (ou bas). Sinon range = pas de trade.
#   2. CRT H4 : la bougie 2 sweep un côté de la bougie 1 et clôture dans son range,
#      uniquement dans le sens de la tendance.
#   3. Zone M15 : FVG (sinon OB) formé pendant la bougie 2.
#   4. Entrée : bougie M15 de rejet de la zone. SL au-delà de sa mèche + marge. TP = swing H4
#      opposé le plus proche non touché.
#   5. Exécution semi-auto : ordre envoyé UNIQUEMENT sur compte DÉMO, après ton appui sur « Executer » dans ntfy.
# ================================================================

APP_ID = os.getenv('DERIV_APP_ID', '').strip()
PUBLIC_URL = os.getenv('PUBLIC_URL', 'https://adonis-production-8b23.up.railway.app').rstrip('/')
REDIRECT_URI = PUBLIC_URL + '/callback'
AUTH_URL = 'https://auth.deriv.com/oauth2/auth'
TOKEN_URL = 'https://auth.deriv.com/oauth2/token'
API_BASE = 'https://api.derivws.com'
TIMEOUT = aiohttp.ClientTimeout(total=25)
ACCOUNT_RE = re.compile(r'^[A-Za-z0-9_-]{1,64}$')
SESSIONS = {}
PENDING = {}
_RAW_NTFY = os.getenv('NTFY_TOPIC', '').strip()
NTFY_TOPIC = _RAW_NTFY if re.match(r'^[A-Za-z0-9_-]{8,64}$', _RAW_NTFY) else ''
PUBLIC_WS = os.getenv('DERIV_PUBLIC_WS', 'wss://api.derivws.com/trading/v1/options/ws/public')
NTFY_URL = 'https://ntfy.sh/' + NTFY_TOPIC if NTFY_TOPIC else ''
PORT = int(os.getenv('PORT', '8080'))
# Protocole de mesure demandé : fenêtre historique fixe de 180 jours.
BT_DAYS = 180
MIN_RR = max(0.0, float(os.getenv('MIN_RR', '1.0')))
LOOKBACK = 300                       # bougies H4 analysées pour tendance et TP
SWING_N = 2                          # bougies de chaque côté pour un swing
TREND_FILTER = os.getenv('TREND_FILTER', '1').strip().lower() not in ('0', 'false', 'off', 'non', 'no')
SL_MARGIN_PCT = float(os.getenv('SL_MARGIN_PCT', '10'))      # marge du SL, % de la bougie 1
SETUP_MAX_M15 = int(os.getenv('SETUP_MAX_M15', '16'))        # 16 = jusqu'à la prochaine bougie H4
BOOM_CRASH_DIR = os.getenv('BOOM_CRASH_DIR', '0').strip() == '1'  # Boom = achats, Crash = ventes
SPREAD_PCT = max(0.0, float(os.getenv('SPREAD_PCT', '0')))        # coût par trade (spread) en % du prix, valeur par défaut du backtest
RISK_PER_TRADE = max(0.01, float(os.getenv('RISK_PER_TRADE', '10')))  # perte en devise du compte si le SL est touché
MAX_STAKE = max(1.0, float(os.getenv('MAX_STAKE', '500')))         # mise maximale par trade
EXEC_TTL = max(1, int(os.getenv('EXEC_TTL_MIN', '10'))) * 60       # validité du bouton Executer
MIN_STAKE = 1.0
MULT_FALLBACK = [1000, 750, 500, 400, 300, 200, 150, 100, 80, 60, 50, 40, 30, 20, 10, 5, 3, 2]
EXEC = {'account_id': None, 'currency': None, 'token': None, 'exp': 0, 'armed_at': None}
PENDING_ORDERS = {}
SPREAD_GRID = [0, 0.005, 0.01, 0.02, 0.03, 0.05, 0.08, 0.1, 0.15, 0.2]  # grille de sensibilité (% du prix)
SCAN_SECONDS = max(30, int(os.getenv('SCAN_EVERY', '60')))
SYMBOLS = [
    ('frxXAUUSD','XAUUSD'), ('cryBTCUSD','BTCUSD'), ('frxEURUSD','EURUSD'),
    ('frxUSDJPY','USDJPY'), ('frxAUDCAD','AUDCAD'), ('1HZ50V','Volatility 50 (1s)'),
    ('BOOM500','Boom 500'), ('CRASH500','Crash 500'), ('CRASH1000','Crash 1000'),
    ('BOOM1000','Boom 1000'), ('JD10','Jump 10'), ('stpRNG','Step Index')
]

STATE = {'scanner':'initialisation','last_scan':None,'last_error':None,'last_signal':None,'symbols':{}}
BT = {'running':False,'done':False,'progress':'','error':None,'results':None}
SIGNALS = deque(maxlen=100)
_SENT = set()
_REQ_ID = 0

HTML = r'''
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ADONIS - Terminal Pro</title>
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
    <h1 style="margin:0; font-size:22px; color:#38bdf8;">⚡ ADONIS PRO</h1>
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
    <h3>🧪 Backtest historique — 180 jours</h3>
    <p class="small">Période fixe : 180 jours calendaires complets avant le lancement du test. Rejoue tes règles sur le passé, marché par marché. Résultats en « R » : 1 R = ce que tu risques sur un trade (perdu = -1 R, gagné = +gain/risque). Les coûts réels (spread, glissement, commission) ne sont pas inclus.</p>
    <label class="small">SPREAD PAR TRADE (% du prix, ex. 0.02)</label>
    <input id="btSpread" type="number" step="0.001" min="0" value="0" style="width:100%; padding:10px; margin-bottom:10px; border-radius:5px; background:#222; color:white; border:1px solid #444; box-sizing:border-box;">
    <button class="btn btn-blue" id="btBtn" onclick="startBacktest()">Lancer le backtest 180 jours</button>
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

  <div class="card" id="execCard" style="display:none;">
    <h3>⚡ Mode semi-auto (démo uniquement)</h3>
    <p class="small">Choisis ton compte démo dans « Compte & Connexion », puis arme le mode. Chaque signal arrive sur ntfy avec un bouton « Executer ». Le serveur refuse tout compte réel.</p>
    <div id="execStatus" class="small" style="margin-bottom:8px;">--</div>
    <div class="row">
      <button class="btn btn-blue" onclick="armExec()">Armer</button>
      <button class="btn btn-red" onclick="disarmExec()">Désarmer</button>
    </div>
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
const $=id=>document.getElementById(id); let ws=null, reqId=1, btTimer=null;
function log(s){const d=document.createElement('div');d.className='log-item';d.textContent=new Date().toLocaleTimeString()+'  '+s;$('log').prepend(d);}
function badge(t,c){$('connectionBadge').textContent=t;$('connectionBadge').style.background=c;}
function send(o){if(ws&&ws.readyState===1){o.req_id=reqId++;ws.send(JSON.stringify(o));}}
function fillMarkets(list,keep){const s=$('marketSelect');s.innerHTML='';list.forEach(x=>{let o=document.createElement('option');o.value=x[0];o.textContent=x[1];s.appendChild(o)});if(keep)s.value=keep;}
function subscribeTicker(){if(!ws||ws.readyState!==1)return;send({forget_all:'ticks'});$('livePrice').textContent='--';send({ticks:$('marketSelect').value,subscribe:1});}
async function init(){
  fillMarkets(defaultMarkets,null);
  try{const me=await(await fetch('/api/me')).json();if(!me.app_id_ok)log('DERIV_APP_ID absent dans Railway > Variables.');
    if(!me.logged_in){$('loginCard').style.display='block';return;}
    $('accountCard').style.display='block';$('btCard').style.display='block';$('scanCard').style.display='block';$('radarCard').style.display='block';$('execCard').style.display='block';
    await loadAccounts();await loadBacktest();await loadScanner();await loadExec();setInterval(()=>{loadScanner();loadExec();},30000);
  }catch(e){log('Erreur serveur : '+e);}
}
async function loadAccounts(){try{const r=await fetch('/api/accounts');const j=await r.json();$('debug').textContent=JSON.stringify(j,null,2);const body=j.body||{};const list=Array.isArray(body)?body:(body.data||body.accounts||[]);const sel=$('accountSelect');sel.innerHTML='';list.forEach(a=>{const id=a.account_id||a.accountId||a.id||a.loginid;if(!id)return;const o=document.createElement('option');o.value=id;o.textContent=[id,a.account_type||a.type||'',a.balance!==undefined?(a.balance+' '+(a.currency||'')):''].filter(Boolean).join(' · ');sel.appendChild(o)});if(!sel.options.length)log('Aucun compte reconnu dans la réponse Deriv.');else log(sel.options.length+' compte(s) Deriv trouvé(s).');}catch(e){log('Chargement comptes : '+e)}}
async function connectWs(){const id=$('accountSelect').value;if(!id){log('Choisis un compte.');return;}closeWs();badge('Connexion…','#eab308');try{const r=await fetch('/api/otp',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({account_id:id})});const j=await r.json();if(!j.url)throw new Error(JSON.stringify(j));ws=new WebSocket(j.url);ws.onopen=()=>{badge('Connecté (Live)','#22c55e');$('brokerDisplay').textContent='Deriv — '+id;log('WebSocket Deriv connecté.');send({balance:1,subscribe:1});send({active_symbols:'brief'});subscribeTicker();};ws.onmessage=e=>{let d;try{d=JSON.parse(e.data)}catch(_){return}if(d.error){log('Deriv : '+d.error.message);return;}if(d.msg_type==='balance'&&d.balance){$('accountBalance').textContent=d.balance.balance+' '+d.balance.currency;}if(d.msg_type==='tick'&&d.tick){$('livePrice').textContent=d.tick.quote;}};ws.onclose=()=>{badge('Déconnecté','#eab308');log('WebSocket Deriv fermé.');};ws.onerror=()=>log('Erreur WebSocket Deriv.');}catch(e){badge('Erreur','#ef4444');log('Connexion Deriv : '+e.message);}}
function closeWs(){if(ws){try{ws.close()}catch(e){}ws=null;}badge('Déconnecté','#eab308');$('brokerDisplay').textContent='Non connecté';$('accountBalance').textContent='--';}
async function loadScanner(){try{const j=await(await fetch('/api/scanner')).json();$('scanMeta').textContent='Dernière analyse : '+(j.derniere_analyse?new Date(j.derniere_analyse*1000).toLocaleTimeString():'en attente')+' · Notify : '+(j.ntfy_etat||'');$('scanList').innerHTML='';(j.signaux||[]).forEach(s=>{let d=document.createElement('div');d.className='log-item';d.textContent='🔔 '+s; $('scanList').appendChild(d)});Object.entries(j.symboles||{}).forEach(([s,v])=>{let d=document.createElement('div');d.className='log-item';d.textContent=(v.label||s)+' — '+v.resume;$('scanList').appendChild(d)});$('scanSymbols').textContent=(j.disponibles||[]).map(x=>x[0]+' = '+x[1]).join('\\n');}catch(e){}}
async function startBacktest(){$('btBtn').disabled=true;$('btStatus').textContent='Démarrage du backtest…';$('btDownload').style.display='none';$('btList').innerHTML='';try{const r=await fetch('/api/backtest/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({spread_pct:parseFloat($('btSpread').value)||0})});const j=await r.json();if(!r.ok)throw new Error(j.error||r.status);if(btTimer)clearInterval(btTimer);btTimer=setInterval(loadBacktest,2500);loadBacktest();}catch(e){$('btStatus').textContent='Erreur : '+e;$('btBtn').disabled=false;}}
const fmt=x=>typeof x==='number'?(x>0?'+':'')+x.toFixed(3):x;
async function loadBacktest(){try{const r=await fetch('/api/backtest');if(r.status===401)return;const j=await r.json();$('btStatus').textContent=j.error?'Erreur : '+j.error:(j.running?'En cours : '+j.progress:(j.done?'Backtest terminé.':'Prêt.'));$('btDownload').style.display=(j.done&&j.results)?'block':'none';if(!j.running){$('btBtn').disabled=false;if(btTimer){clearInterval(btTimer);btTimer=null;}}if(!j.results)return;const p=j.results.portfolio||{};$('btList').innerHTML='';let head=document.createElement('div');head.className='log-item';head.textContent=`PORTFEUILLE : ${p.trades} trades · ${p.wins} gagnés · ${p.losses} perdus · ${fmt(p.r_total)} R · PF ${p.pf} · DD ${p.max_dd_r} R · spread max ${j.results.break_even_spread_pct} %`; $('btList').appendChild(head);(j.results.markets||[]).forEach(x=>{let d=document.createElement('div');d.className='log-item';d.textContent=x.name+' : '+(x.error?'ERREUR — '+x.error:`${x.trades} trades · ${fmt(x.r_total)} R · PF ${x.pf} · DD ${x.max_dd_r} R · spread max ${x.break_even_spread_pct} %`);$('btList').appendChild(d)});if(j.results.incomplete_markets&&j.results.incomplete_markets.length){let d=document.createElement('div');d.className='small';d.textContent='Attention : backtest partiel, marchés en erreur : '+j.results.incomplete_markets.join(', ');$('btList').appendChild(d);}}catch(e){$('btStatus').textContent='Erreur de lecture : '+e;}}
async function loadExec(){try{const j=await(await fetch('/api/exec/status')).json();$('execStatus').textContent=j.armed?('ARMÉ sur '+j.account_id+' (démo) · expire à '+new Date(j.expires*1000).toLocaleTimeString()+' · risque '+j.risk+' par trade · bouton valable '+j.ttl_min+' min'):'Non armé : aucune exécution possible.';}catch(e){}}
async function armExec(){const id=$('accountSelect').value;if(!id){log('Choisis un compte.');return;}try{const r=await fetch('/api/exec/arm',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({account_id:id})});const j=await r.json();log(r.ok?'Mode semi-auto armé (démo).':'Armement refusé : '+(j.error||r.status));}catch(e){log('Armement : '+e);}loadExec();}
async function disarmExec(){try{await fetch('/api/exec/disarm',{method:'POST'});log('Mode semi-auto désarmé.');}catch(e){}loadExec();}
const defaultMarkets=[['frxXAUUSD','XAUUSD'],['cryBTCUSD','BTCUSD'],['frxEURUSD','EURUSD'],['frxUSDJPY','USDJPY'],['frxAUDCAD','AUDCAD'],['1HZ50V','Volatility 50 (1s)'],['BOOM500','Boom 500'],['CRASH500','Crash 500'],['CRASH1000','Crash 1000'],['BOOM1000','Boom 1000'],['JD10','Jump 10'],['stpRNG','Step Index']];
init();
</script>
</body>
</html>
'''

class DerivError(Exception): pass

_REQ_ID = 0
_DERIV_LAST_REQUEST = 0.0
_DERIV_MIN_INTERVAL = 0.35

async def connect_ws(session):
    return await session.ws_connect(PUBLIC_WS, heartbeat=25, timeout=30)

async def request(ws, payload, timeout=35):
    global _REQ_ID, _DERIV_LAST_REQUEST
    wait = _DERIV_MIN_INTERVAL - (time.monotonic() - _DERIV_LAST_REQUEST)
    if wait > 0:
        await asyncio.sleep(wait)
    _DERIV_LAST_REQUEST = time.monotonic()
    _REQ_ID += 1
    rid = _REQ_ID
    await ws.send_str(json.dumps(dict(payload, req_id=rid)))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msg = await ws.receive(timeout=max(1, deadline-time.monotonic()))
        if msg.type == aiohttp.WSMsgType.TEXT:
            try: obj = json.loads(msg.data)
            except Exception: continue
            if obj.get('req_id') == rid:
                if obj.get('error'):
                    raise DerivError(obj['error'].get('message', 'Erreur Deriv'))
                return obj
        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING):
            raise ConnectionError('Deriv a fermé le WebSocket')
    raise asyncio.TimeoutError('Délai de réponse Deriv dépassé')

async def request_resilient(session, ws_holder, payload, retries=4):
    last = None; rate_waits = 0; attempt = 0
    while attempt < retries:
        try:
            if ws_holder[0] is None or ws_holder[0].closed:
                ws_holder[0] = await connect_ws(session)
            return await request(ws_holder[0], payload)
        except DerivError as exc:
            # Limite de débit Deriv (ex. ticks_history_expensive) : on attend puis on réessaie.
            if 'rate limit' in str(exc).lower() and rate_waits < 6:
                rate_waits += 1
                await asyncio.sleep(15 * rate_waits)
                continue
            raise
        except (ConnectionError, aiohttp.ClientError, asyncio.TimeoutError, OSError, RuntimeError) as exc:
            last = exc
            old = ws_holder[0]
            ws_holder[0] = None
            if old is not None:
                try: await old.close()
                except Exception: pass
            attempt += 1
            if attempt < retries:
                await asyncio.sleep(min(8, 1.5 * attempt))
    raise ConnectionError('Connexion Deriv indisponible après reprises : ' + str(last))


def normalize(raw):
    out=[]
    for c in raw or []:
        try:
            x={'t':int(c.get('epoch',c.get('t'))),'o':float(c.get('open',c.get('o'))),'h':float(c.get('high',c.get('h'))),'l':float(c.get('low',c.get('l'))),'c':float(c.get('close',c.get('c')))}
            if all(math.isfinite(x[k]) for k in ('o','h','l','c')) and x['l']<=min(x['o'],x['c'])<=max(x['o'],x['c'])<=x['h']: out.append(x)
        except Exception: pass
    out.sort(key=lambda x:x['t'])
    dedup={x['t']:x for x in out}
    return [dedup[t] for t in sorted(dedup)]

async def fetch_history(session, ws_holder, symbol, granularity, start, end, page_size=1000):
    """Pages chronologiques; reconnecte le WS si Deriv ferme le transport."""
    cursor_end = 'latest'
    allc = {}
    pages = 0
    while pages < 35:
        remaining = max(0, int((end-start)//granularity) - len(allc) + 3)
        count = min(page_size, max(1, remaining))
        payload = {'ticks_history': symbol, 'end': cursor_end, 'style': 'candles', 'granularity': granularity, 'count': count}
        if cursor_end == 'latest': payload['adjust_start_time'] = 1
        resp = await request_resilient(session, ws_holder, payload)
        batch = normalize(resp.get('candles'))
        if not batch: break
        # Validate page timestamps and OHLC; no silently accepting future/out-of-window bars.
        for c in batch:
            if c['t'] % granularity != 0:
                continue
            if c['t'] >= start and c['t'] + granularity <= end:
                old = allc.get(c['t'])
                if old and any(old[k] != c[k] for k in ('o','h','l','c')):
                    raise DerivError(f'OHLC divergent à {c["t"]}')
                allc[c['t']] = c
        oldest = min(x['t'] for x in batch)
        if oldest <= start: break
        if cursor_end != 'latest' and oldest >= int(cursor_end):
            raise DerivError('Pagination Deriv sans progression temporelle')
        cursor_end = oldest - 1
        pages += 1
        # Une page plus courte que count ne signifie pas nécessairement
        # que l'historique est terminé : Deriv peut limiter la réponse.
        # On continue tant que le curseur recule et que start n'est pas atteint.
        await asyncio.sleep(0.12)
    result = [allc[t] for t in sorted(allc)]
    expected = max(1, int((end-start)//granularity))
    # Contrôle de couverture : FX ferme les week-ends, les marchés continus non.
    # On refuse de qualifier le test de complet si une part excessive des bougies manque.
    # FX : plancher de 60 % des bougies calendaires (week-ends/jours fériés exclus).
    # Crypto/synthétiques : plancher de 90 % (marchés normalement continus).
    coverage_floor = 0.60 if symbol.startswith('frx') else 0.90
    min_required = int(expected * coverage_floor)
    if len(result) < min_required:
        pct = 100.0 * len(result) / expected if expected else 0.0
        raise DerivError(
            f'historique incomplet : {len(result)}/{expected} bougies '
            f'({pct:.1f} %, minimum {coverage_floor*100:.0f} % requis)'
        )
    return result

async def fetch_latest(session, ws_holder, symbol, granularity, count):
    resp = await request_resilient(session, ws_holder, {'ticks_history': symbol, 'end': 'latest', 'style': 'candles', 'granularity': granularity, 'count': count, 'adjust_start_time': 1})
    now = int(time.time())
    return [c for c in normalize(resp.get('candles')) if c['t'] + granularity <= now]



# ================================================================
# MOTEUR DE STRATÉGIE — CRT H4 + retest FVG/OB M15
# ================================================================
def swings(cs, n=SWING_N):
    """Swings stricts : un sommet/creux dépasse les n bougies de chaque côté."""
    highs, lows = [], []
    for i in range(n, len(cs) - n):
        h = cs[i]['h']; lo = cs[i]['l']
        around = [j for j in range(i - n, i + n + 1) if j != i]
        if all(h > cs[j]['h'] for j in around): highs.append((i, h))
        if all(lo < cs[j]['l'] for j in around): lows.append((i, lo))
    return highs, lows

def h4_trend(cs):
    """1 = haussier, -1 = baissier, 0 = range."""
    hs, ls = swings(cs)
    if len(hs) < 2 or len(ls) < 2: return 0
    if hs[-1][1] > hs[-2][1] and ls[-1][1] > ls[-2][1]: return 1
    if hs[-1][1] < hs[-2][1] and ls[-1][1] < ls[-2][1]: return -1
    return 0

def find_tp(cs, side, ref):
    """Swing H4 opposé le plus proche qui n'a pas encore été touché."""
    hs, ls = swings(cs)
    best = None
    if side == 'SELL':
        for i, v in ls:
            if v < ref and not any(c['l'] <= v for c in cs[i + 1:]):
                if best is None or v > best: best = v
    else:
        for i, v in hs:
            if v > ref and not any(c['h'] >= v for c in cs[i + 1:]):
                if best is None or v < best: best = v
    return best

def find_zone(seg, side):
    """FVG d'abord (premier trou après l'extrême du sweep), sinon OB. seg = M15 de la bougie 2."""
    n = len(seg)
    if n < 3: return None
    ext = 0
    for i in range(1, n):
        if side == 'SELL' and seg[i]['h'] >= seg[ext]['h']: ext = i
        if side == 'BUY' and seg[i]['l'] <= seg[ext]['l']: ext = i
    for i in range(ext + 2, n):
        a, c = seg[i - 2], seg[i]
        if side == 'SELL' and c['h'] < a['l']: return {'kind': 'FVG', 'lo': c['h'], 'hi': a['l']}
        if side == 'BUY' and c['l'] > a['h']: return {'kind': 'FVG', 'lo': a['h'], 'hi': c['l']}
    for j in range(ext, -1, -1):
        x = seg[j]
        if (side == 'SELL' and x['c'] > x['o']) or (side == 'BUY' and x['c'] < x['o']):
            return {'kind': 'OB', 'lo': x['l'], 'hi': x['h']}
    return None

def build_setup(cs, seg, sym=''):
    """cs = bougies H4 clôturées (la dernière est la bougie 2). seg = M15 pendant la bougie 2."""
    out = {'trend': 0, 'status': 'données insuffisantes', 'setup': None}
    if len(cs) < 12: return out
    c1, c2 = cs[-2], cs[-1]
    trend = h4_trend(cs)
    out['trend'] = trend
    side = None
    if c2['h'] > c1['h'] and c2['l'] >= c1['l'] and c1['l'] < c2['c'] < c1['h']: side = 'SELL'
    elif c2['l'] < c1['l'] and c2['h'] <= c1['h'] and c1['l'] < c2['c'] < c1['h']: side = 'BUY'
    if side is None:
        out['status'] = 'pas de CRT'; return out
    if TREND_FILTER and trend != (1 if side == 'BUY' else -1):
        out['status'] = 'CRT refusé (tendance)'; return out
    if BOOM_CRASH_DIR:
        if 'BOOM' in sym.upper() and side == 'SELL': out['status'] = 'vente Boom ignorée'; return out
        if 'CRASH' in sym.upper() and side == 'BUY': out['status'] = 'achat Crash ignoré'; return out
    tp = find_tp(cs, side, c2['c'])
    if tp is None:
        out['status'] = 'TP introuvable'; return out
    zone = find_zone(seg, side)
    if zone is None:
        out['status'] = 'zone M15 introuvable'; return out
    out['setup'] = {'side': side, 'c1hi': c1['h'], 'c1lo': c1['l'], 'zone': zone, 'tp': tp,
                    'c2_time': c2['t'], 'arm_time': c2['t'] + 14400}
    out['status'] = ('VENTE' if side == 'SELL' else 'ACHAT') + ' armé(e) : attente du retour dans ' + zone['kind']
    return out

def find_entry(setup, bars):
    """bars = M15 clôturées après la bougie 2, jusqu'à la prochaine bougie H4.
    Retourne (signal, statut). Entrée à la clôture de la bougie de rejet."""
    side = setup['side']; z = setup['zone']; tp = setup['tp']
    margin = (setup['c1hi'] - setup['c1lo']) * SL_MARGIN_PCT / 100.0
    for c in bars:
        if c['t'] > setup['arm_time'] + SETUP_MAX_M15 * 900:
            return None, 'setup expiré'
        if side == 'SELL':
            if c['l'] <= tp: return None, 'TP touché avant entrée'
            if c['c'] > z['hi']: return None, 'zone cassée'
            if not (c['h'] >= z['lo'] and c['c'] < z['hi'] and c['c'] < c['o']): continue
            entry = c['c']; sl = c['h'] + margin
            risk = sl - entry; reward = entry - tp
        else:
            if c['h'] >= tp: return None, 'TP touché avant entrée'
            if c['c'] < z['lo']: return None, 'zone cassée'
            if not (c['l'] <= z['hi'] and c['c'] > z['lo'] and c['c'] > c['o']): continue
            entry = c['c']; sl = c['l'] - margin
            risk = entry - sl; reward = tp - entry
        if risk <= 0 or reward <= 0 or reward / risk < MIN_RR:
            return None, 'RR insuffisant'
        return {'side': side, 'zone': z['kind'], 'zone_lo': z['lo'], 'zone_hi': z['hi'],
                'entry': round(entry, 5), 'sl': round(sl, 5), 'tp': round(tp, 5),
                'rr': round(reward / risk, 3), 'time': c['t'], 'crt_time': setup['arm_time']}, 'signal'
    return None, 'en attente du retour dans la zone'

def scan_closed(h4, m15, sym=''):
    """Scanner temps réel. Retourne (signal ou None, statut, tendance)."""
    cs = h4[-LOOKBACK:]
    if len(cs) < 12 or len(m15) < 30: return None, 'données insuffisantes', 0
    c2 = cs[-1]
    seg = [c for c in m15 if c2['t'] <= c['t'] < c2['t'] + 14400]
    info = build_setup(cs, seg, sym)
    st = info['setup']
    if not st: return None, info['status'], info['trend']
    bars = [c for c in m15 if st['arm_time'] <= c['t'] < st['arm_time'] + 14400]
    sig, status = find_entry(st, bars)
    if sig and sig['time'] == m15[-1]['t']: return sig, 'SIGNAL', info['trend']
    if sig: status = 'signal déjà passé'
    return None, status, info['trend']


# ================================================================
# EXÉCUTION SEMI-AUTO — compte DÉMO uniquement, via contrats Multipliers (MULTUP/MULTDOWN)
# ================================================================
def is_demo_url(url):
    return isinstance(url, str) and '/ws/demo' in url and '/ws/real' not in url

def account_list(body):
    if isinstance(body, dict):
        body = body.get('data', body.get('accounts', body))
        if isinstance(body, dict): body = body.get('accounts', body.get('data', []))
    return body if isinstance(body, list) else []

def account_id_of(a):
    return str(a.get('account_id') or a.get('accountId') or a.get('id') or a.get('loginid') or '')

def make_order_token(sym, name, sig):
    now = time.time()
    if not (EXEC['account_id'] and EXEC['exp'] > now): return None
    for k in [k for k, v in PENDING_ORDERS.items() if v['exp'] < now - 3600]: PENDING_ORDERS.pop(k, None)
    tok = secrets.token_urlsafe(24)
    PENDING_ORDERS[tok] = {'sym': sym, 'name': name, 'sig': sig, 'exp': now + EXEC_TTL, 'used': False}
    return tok

def plan_amounts(spot, sl, tp, risk, mult):
    """Multiplier : gain/perte = mise x multiplicateur x variation relative du prix.
    On choisit la mise pour que la perte au SL égale `risk`."""
    sl_pct = abs(spot - sl) / spot; tp_pct = abs(tp - spot) / spot
    stake = risk / (mult * sl_pct)
    return round(stake, 2), round(risk, 2), round(stake * mult * tp_pct, 2)

async def place_multiplier(ws, sym, sig, currency):
    side = sig['side']; sl = sig['sl']; tp = sig['tp']
    ctype = 'MULTUP' if side == 'BUY' else 'MULTDOWN'
    cf = await request(ws, {'contracts_for': sym})
    avail = (cf.get('contracts_for') or {}).get('available') or []
    items = [c for c in avail if c.get('contract_type') == ctype]
    if not items:
        return {'ok': False, 'msg': f'{sym} : pas de contrat Multiplier disponible, exécution impossible.'}
    rng = items[0].get('multiplier_range')
    mults = sorted({float(x) for x in rng if x}, reverse=True) if isinstance(rng, list) and rng else MULT_FALLBACK
    cd = await request(ws, {'ticks_history': sym, 'end': 'latest', 'style': 'candles', 'granularity': 60, 'count': 1, 'adjust_start_time': 1})
    cc = normalize(cd.get('candles'))
    if not cc: return {'ok': False, 'msg': 'Prix actuel indisponible.'}
    spot = cc[-1]['c']
    if (side == 'BUY' and not (sl < spot < tp)) or (side == 'SELL' and not (tp < spot < sl)):
        return {'ok': False, 'msg': f'Prix actuel {spot} hors de la zone SL/TP : signal périmé.'}
    rr_now = abs(tp - spot) / abs(spot - sl)
    if rr_now < MIN_RR:
        return {'ok': False, 'msg': f'RR dégradé à {rr_now:.2f} (minimum {MIN_RR}) : signal périmé.'}
    sl_pct = abs(spot - sl) / spot
    tried = 0; last = ''
    for m in mults:
        if m * sl_pct > 0.95: continue          # le stop-out tomberait avant ton SL
        stake, sl_amt, tp_amt = plan_amounts(spot, sl, tp, RISK_PER_TRADE, m)
        if stake < MIN_STAKE: continue
        if stake > MAX_STAKE:
            last = f'mise {stake} > MAX_STAKE {MAX_STAKE}'; break
        tried += 1
        if tried > 6: break
        try:
            pr = await request(ws, {'proposal': 1, 'amount': stake, 'basis': 'stake', 'contract_type': ctype,
                                    'currency': currency, 'multiplier': m, 'underlying_symbol': sym,
                                    'limit_order': {'stop_loss': sl_amt, 'take_profit': tp_amt}})
        except DerivError as exc:
            last = str(exc); continue
        pp = pr.get('proposal') or {}
        pid = pp.get('id'); ask = pp.get('ask_price')
        if not pid or ask is None: last = 'proposition invalide'; continue
        if ask > MAX_STAKE * 1.05: last = f'prix {ask} > MAX_STAKE'; continue
        by = await request(ws, {'buy': pid, 'price': ask})
        b = by.get('buy') or {}
        return {'ok': True, 'msg': (f"{sig['side']} exécuté (démo) | x{m:g} | mise {stake} {currency} | "
                                    f"perte max au SL {sl_amt} | gain au TP {tp_amt} | contrat {b.get('contract_id', '?')}")}
    if tried == 0 and not last: last = 'aucun multiplicateur compatible avec la distance du SL'
    return {'ok': False, 'msg': 'Aucun multiplicateur accepté. Dernière erreur : ' + last}

async def execute_order(p):
    if not (EXEC['account_id'] and EXEC['exp'] > time.time()):
        return {'ok': False, 'msg': 'Session Deriv expirée ou mode non armé : reconnecte-toi dans l\'app et réarme.'}
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as http:
            async with http.post(API_BASE + '/trading/v1/options/accounts/' + EXEC['account_id'] + '/otp',
                                 headers=deriv_headers(EXEC['token'])) as r:
                status = r.status; raw = await r.text()
            body = parse_json(raw)
            url = body.get('data', {}).get('url') if isinstance(body, dict) else None
            if not (200 <= status < 300 and url):
                return {'ok': False, 'msg': f'OTP refusé (HTTP {status}) : reconnecte-toi dans l\'app.'}
            if not is_demo_url(url):
                return {'ok': False, 'msg': 'Compte non démo : exécution refusée.'}
            ws = await http.ws_connect(url, heartbeat=25, timeout=30)
            try:
                return await place_multiplier(ws, p['sym'], p['sig'], EXEC['currency'])
            finally:
                try: await ws.close()
                except Exception: pass
    except Exception as exc:
        return {'ok': False, 'msg': f'Erreur : {type(exc).__name__}: {str(exc)[:160]}'}

async def send_notify(title, message, actions=None):
    if not NTFY_URL:return False
    try:
        timeout=aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post(NTFY_URL,data=message.encode(),headers=({'Title':title,'Priority':'high'} | ({'Actions':actions} if actions else {}))) as r:
                return 200<=r.status<300
    except Exception as e:
        print('Notify error:',repr(e));return False

async def scanner_loop(app):
    await asyncio.sleep(8)
    while True:
        if BT['running']:
            await asyncio.sleep(5)
            continue
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=50)) as session:
                for sym, name in SYMBOLS:
                    if BT['running']: break
                    holder = [None]
                    try:
                        h4 = await fetch_latest(session, holder, sym, 14400, 400)
                        m15 = await fetch_latest(session, holder, sym, 900, 800)
                        sig, status, trend = scan_closed(h4, m15, sym)
                        if sig:
                            key = (sym, sig['side'], sig['time'])
                            if key not in _SENT:
                                _SENT.add(key)
                                msg = (f"{name} {sig['side']} | {sig['zone']} | entrée {sig['entry']} | SL {sig['sl']} | TP {sig['tp']} | RR {sig['rr']}")
                                SIGNALS.appendleft(msg); STATE['last_signal'] = msg
                                tok = make_order_token(sym, name, sig)
                                act = f"http, Executer, {PUBLIC_URL}/api/exec/run/{tok}, method=POST, clear=true" if tok else None
                                extra = (f"\nAppuie sur Executer (valable {EXEC_TTL // 60} min, compte demo)" if tok else "\nMode semi-auto non arme")
                                await send_notify('ADONIS — signal', msg + extra, act)
                        tr = {1: 'Haussier', -1: 'Baissier'}.get(trend, 'Range')
                        STATE['symbols'][sym] = {'label': name, 'resume': tr + ' · ' + status}
                    except Exception as exc:
                        STATE['symbols'][sym] = {'label': name, 'resume': 'données indisponibles : ' + str(exc)[:100]}
                        STATE['last_error'] = f'{name}: {exc}'[:220]
                    finally:
                        if holder[0] is not None:
                            try: await holder[0].close()
                            except Exception: pass
                    await asyncio.sleep(0.15)
            STATE['scanner'] = 'actif'
            STATE['last_scan'] = time.time()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            STATE['scanner'] = 'erreur'; STATE['last_error'] = str(exc)[:220]
            print('Scanner:', repr(exc), flush=True)
        await asyncio.sleep(SCAN_SECONDS)


def simulate_trade(m15, idx, sig):
    """Rejoue le trade après l'entrée. SL prioritaire si SL et TP sont touchés dans la même bougie."""
    side = sig['side']; entry = sig['entry']; sl = sig['sl']; tp = sig['tp']
    risk = abs(entry - sl)
    if risk <= 0: return None
    for k in range(idx + 1, len(m15)):
        c = m15[k]
        if side == 'BUY':
            if c['l'] <= sl: return {'r': -1.0, 'outcome': 'SL', 'exit_time': c['t']}
            if c['h'] >= tp: return {'r': round((tp - entry) / risk, 4), 'outcome': 'TP', 'exit_time': c['t']}
        else:
            if c['h'] >= sl: return {'r': -1.0, 'outcome': 'SL', 'exit_time': c['t']}
            if c['l'] <= tp: return {'r': round((entry - tp) / risk, 4), 'outcome': 'TP', 'exit_time': c['t']}
    return {'r': 0.0, 'outcome': 'OPEN', 'exit_time': m15[-1]['t']}

def stats(trades):
    vals=[x['r'] for x in trades]; wins=sum(x for x in vals if x>0); losses=-sum(x for x in vals if x<0)
    eq=peak=dd=0.0
    for x in vals:
        eq+=x;peak=max(peak,eq);dd=max(dd,peak-eq)
    return {'trades':len(vals),'wins':sum(x>0 for x in vals),'losses':sum(x<0 for x in vals),'open':sum(x==0 for x in vals),'r_total':round(sum(vals),3),'expectancy_r':round(sum(vals)/len(vals),4) if vals else 0,'pf':round(wins/losses,3) if losses else ('inf' if wins else 0),'max_dd_r':round(dd,3)}

def backtest_symbol(h4, m15, sym, name, start, spread_pct=0.0):
    """Une seule position à la fois par marché. Entrée à la clôture de la bougie de rejet."""
    trades = []; last_exit = 0
    m15_t = [c['t'] for c in m15]
    for j in range(12, len(h4)):
        c2 = h4[j]
        if c2['t'] < start: continue
        close_t = c2['t'] + 14400
        cs = h4[max(0, j - LOOKBACK + 1):j + 1]
        a = bisect_left(m15_t, c2['t']); b = bisect_left(m15_t, close_t); e = bisect_left(m15_t, close_t + 14400)
        info = build_setup(cs, m15[a:b], sym)
        st = info['setup']
        if not st: continue
        sig, _ = find_entry(st, m15[b:e])
        if not sig or sig['time'] <= last_exit: continue
        idx = bisect_left(m15_t, sig['time'])
        sim = simulate_trade(m15, idx, sig)
        if not sim: continue
        risk_pct = abs(sig['entry'] - sig['sl']) / sig['entry'] * 100.0
        gross = sim['r']
        # Coût du spread payé une fois par trade clos : spread% / distance du SL en % = coût en R.
        net = gross if sim['outcome'] == 'OPEN' else gross - spread_pct / risk_pct
        sim = {**sim, 'r': round(net, 4)}
        trades.append({**sim, 'r_gross': gross, 'risk_pct': round(risk_pct, 5),
                       'symbol': sym, 'name': name, 'side': sig['side'], 'zone': sig['zone'],
                       'entry': sig['entry'], 'sl': sig['sl'], 'tp': sig['tp'], 'rr': sig['rr'],
                       'signal_time': sig['time']})
        last_exit = sim['exit_time']
    return trades

def break_even_pct(trades):
    """Spread (en % du prix) pour lequel le total en R tombe à zéro. Négatif = déjà perdant sans coût."""
    closed = [t for t in trades if t['outcome'] != 'OPEN' and t.get('risk_pct')]
    k = sum(1.0 / t['risk_pct'] for t in closed)
    if k <= 0: return None
    return round(sum(t['r_gross'] for t in closed) / k, 4)

def cost_sensitivity(trades):
    rows = []
    for pct in SPREAD_GRID:
        vals = [t['r_gross'] if (t['outcome'] == 'OPEN' or not t.get('risk_pct'))
                else t['r_gross'] - pct / t['risk_pct'] for t in trades]
        s = stats([{'r': v} for v in vals])
        rows.append({'spread_pct': pct, 'r_total': s['r_total'], 'expectancy_r': s['expectancy_r'], 'pf': s['pf']})
    return rows

async def run_backtest(spread_pct=0.0):
    spread_pct = max(0.0, float(spread_pct))
    BT.update(running=True, done=False, progress='Préparation des 12 marchés…', error=None, results=None)
    start = int(time.time()) - BT_DAYS * 86400
    end = int(time.time())
    markets, all_trades = [], []
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=75)) as session:
            for n, (sym, name) in enumerate(SYMBOLS, 1):
                BT['progress'] = f'{n}/12 — {name} : récupération H4 et M15'
                holder = [None]
                try:
                    h4 = await fetch_history(session, holder, sym, 14400, start - 45*86400, end)
                    m15 = await fetch_history(session, holder, sym, 900, start, end)
                    BT['progress'] = f'{n}/12 — {name} : replay de la stratégie'
                    trades = backtest_symbol(h4, m15, sym, name, start, spread_pct)
                    all_trades.extend(trades)
                    h4_expected = max(1, int((end - (start - 45*86400)) // 14400))
                    m15_expected = max(1, int((end - start) // 900))
                    markets.append({'symbol':sym, 'name':name, **stats(trades), 'break_even_spread_pct':break_even_pct(trades), 'error':None,
                                    'h4_candles':len(h4), 'm15_candles':len(m15),
                                    'h4_coverage_pct':round(100*len(h4)/h4_expected, 1),
                                    'm15_coverage_pct':round(100*len(m15)/m15_expected, 1)})
                except Exception as exc:
                    print(f'[BACKTEST] {name} error: {type(exc).__name__}: {exc}', flush=True)
                    markets.append({'symbol':sym, 'name':name, 'error':f'{type(exc).__name__}: {str(exc)[:200]}', **stats([])})
                finally:
                    if holder[0] is not None:
                        try: await holder[0].close()
                        except Exception: pass
                await asyncio.sleep(3.0)
        portfolio = stats(sorted(all_trades, key=lambda x:(x['exit_time'],x['signal_time'])))
        incomplete = [x['name'] for x in markets if x.get('error')]
        BT['results'] = {
            'generated_at':datetime.now(timezone.utc).isoformat(), 'period_days':BT_DAYS,
            'period_label':'180 jours calendaires',
            'window_start':start, 'window_end':end,
            'window_start_iso':datetime.fromtimestamp(start, timezone.utc).isoformat(),
            'window_end_iso':datetime.fromtimestamp(end, timezone.utc).isoformat(),
            'strategy':'ADONIS — CRT H4 + retest FVG/OB M15 (tendance H4, rejet, SL sous la mèche du retest, TP swing opposé non touché)',
            'parameters':{'spread_pct':spread_pct, 'trend_filter':TREND_FILTER, 'swing_bars':SWING_N, 'sl_margin_pct_of_c1':SL_MARGIN_PCT,
                          'min_rr':MIN_RR, 'setup_max_m15_bars':SETUP_MAX_M15, 'boom_crash_direction':BOOM_CRASH_DIR},
            'status':'PARTIAL' if incomplete else 'COMPLETE',
            'markets_total':len(SYMBOLS), 'markets_success':len(SYMBOLS)-len(incomplete),
            'incomplete_markets':incomplete, 'markets':markets, 'portfolio':portfolio,
            'spread_pct_used':spread_pct, 'break_even_spread_pct':break_even_pct(all_trades),
            'cost_sensitivity':cost_sensitivity(all_trades),
            'trades':all_trades,
            'notes':['Aucun ordre réel.', f'Spread appliqué : {spread_pct} % du prix par trade (coût en R = spread % / distance du SL %). Glissement et commission non inclus.',
                     'break_even_spread_pct = spread maximum (en % du prix) avant que le total tombe à zéro; négatif = perdant même sans coût.',
                     'Entrée simulée à la clôture de la bougie M15 de rejet.',
                     'SL prioritaire si SL et TP sont touchés dans la même bougie M15.',
                     'Une seule position à la fois par marché; pas de limite globale de positions.',
                     f'MIN_RR={MIN_RR:.2f}; TREND_FILTER={TREND_FILTER}; variables configurables dans Railway.',
                     'Un résultat PARTIAL ne doit pas être interprété comme un backtest complet.',
                     'Couverture minimale exigée : 60 % FX (week-ends/jours fériés), 90 % crypto/synthétiques.',
                     'Le contexte H4 inclut 45 jours supplémentaires avant la fenêtre de performance de 180 jours.']
        }
        BT.update(done=True, progress=('Terminé — résultats partiels' if incomplete else 'Terminé — 12/12 marchés'))
    except Exception as exc:
        BT['error'] = f'{type(exc).__name__}: {exc}'[:300]
    finally:
        BT['running'] = False


def b64url(data): return base64.urlsafe_b64encode(data).rstrip(b'=').decode()
def cleanup_sessions():
    now=time.time()
    for k in [k for k,v in PENDING.items() if now-v['ts']>600]: PENDING.pop(k,None)
    for k in [k for k,v in SESSIONS.items() if v['exp']<now]: SESSIONS.pop(k,None)
def get_session(request):
    sid=request.cookies.get('sid',''); s=SESSIONS.get(sid)
    return (sid,s) if s and s['exp']>time.time() else (None,None)
def parse_json(raw):
    try:return json.loads(raw)
    except Exception:return {'raw':raw[:500]}
def error_page(message,status=400):
    return web.Response(text="<body style='font-family:sans-serif;background:#0f1117;color:white;padding:20px'><h3>Erreur Deriv</h3><p>"+html.escape(message)+"</p><a href='/'>Retour</a></body>",status=status,content_type='text/html')
async def index(request): return web.Response(text=HTML,content_type='text/html',headers={'Cache-Control':'no-store'})
async def login(request):
    if not APP_ID:return error_page('DERIV_APP_ID absent. Ajoute-le dans Railway > Variables.',500)
    cleanup_sessions(); verifier=base64.urlsafe_b64encode(secrets.token_bytes(48)).rstrip(b'=').decode()
    challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode()
    state=secrets.token_urlsafe(24); PENDING[state]={'verifier':verifier,'ts':time.time()}
    params={'response_type':'code','client_id':APP_ID,'redirect_uri':REDIRECT_URI,'scope':'trade','state':state,'code_challenge':challenge,'code_challenge_method':'S256'}
    resp=web.Response(status=302,headers={'Location':AUTH_URL+'?'+urlencode(params)})
    resp.set_cookie('oauth_state',state,max_age=600,httponly=True,secure=True,samesite='Lax'); return resp
async def callback(request):
    q=request.query
    if 'error' in q:return error_page('Deriv a refusé : '+q.get('error','')+' '+q.get('error_description',''))
    state=q.get('state',''); code=q.get('code',''); pending=PENDING.pop(state,None)
    if not pending or not code or state!=request.cookies.get('oauth_state'):return error_page('Vérification de sécurité échouée. Recommence la connexion.')
    form={'grant_type':'authorization_code','client_id':APP_ID,'code':code,'code_verifier':pending['verifier'],'redirect_uri':REDIRECT_URI}
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as http:
            async with http.post(TOKEN_URL,data=form) as r: status=r.status; raw=await r.text()
    except Exception as exc:return error_page('Échange du token impossible : '+str(exc),502)
    data=parse_json(raw)
    if status!=200 or not isinstance(data,dict) or 'access_token' not in data:return error_page('Deriv a refusé le token (HTTP '+str(status)+').',502)
    expires=int(data.get('expires_in',3600)); sid=secrets.token_urlsafe(32); SESSIONS[sid]={'token':data['access_token'],'exp':time.time()+expires}
    resp=web.Response(status=302,headers={'Location':'/'}); resp.set_cookie('sid',sid,max_age=min(expires,604800),httponly=True,secure=True,samesite='Lax'); resp.del_cookie('oauth_state'); return resp
async def logout(request):
    sid,_=get_session(request)
    if sid:SESSIONS.pop(sid,None)
    resp=web.Response(status=302,headers={'Location':'/'});resp.del_cookie('sid');return resp
async def api_me(request):
    _,s=get_session(request);return web.json_response({'logged_in':bool(s),'app_id_ok':bool(APP_ID)})
def deriv_headers(token):return {'Authorization':'Bearer '+token,'Deriv-App-ID':APP_ID}
async def api_accounts(request):
    _,s=get_session(request)
    if not s:return web.json_response({'error':'not_logged_in'},status=401)
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as http:
            async with http.get(API_BASE+'/trading/v1/options/accounts',headers=deriv_headers(s['token'])) as r: status=r.status; raw=await r.text()
    except Exception as exc:return web.json_response({'error':'network','detail':str(exc)},status=502)
    return web.json_response({'status':status,'body':parse_json(raw)})
async def api_otp(request):
    _,s=get_session(request)
    if not s:return web.json_response({'error':'not_logged_in'},status=401)
    try:payload=await request.json()
    except Exception:payload={}
    account=str(payload.get('account_id',''))
    if not ACCOUNT_RE.match(account):return web.json_response({'error':'account_id invalide'},status=400)
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as http:
            async with http.post(API_BASE+'/trading/v1/options/accounts/'+account+'/otp',headers=deriv_headers(s['token'])) as r: status=r.status; raw=await r.text()
    except Exception as exc:return web.json_response({'error':'network','detail':str(exc)},status=502)
    body=parse_json(raw); url=body.get('data',{}).get('url') if isinstance(body,dict) else None
    if 200<=status<300 and url:return web.json_response({'url':url})
    return web.json_response({'error':'otp_failed','status':status,'body':body},status=502)
async def api_scanner(request):
    _,s=get_session(request)
    if not s:return web.json_response({'error':'not_logged_in'},status=401)
    return web.json_response({'derniere_analyse':STATE['last_scan'],'erreur':STATE['last_error'],'ntfy':bool(NTFY_TOPIC),'ntfy_etat':('actives' if NTFY_TOPIC else 'non configurées'),'symboles':STATE.get('symbols',{}),'signaux':list(SIGNALS),'disponibles':[[x,y] for x,y in SYMBOLS]})
async def start_bt(request):
    _,s=get_session(request)
    if not s:return web.json_response({'error':'not_logged_in'},status=401)
    try:payload=await request.json()
    except Exception:payload={}
    try:pct=max(0.0,min(5.0,float(payload.get('spread_pct',SPREAD_PCT))))
    except Exception:pct=SPREAD_PCT
    if not BT['running']:request.app['bt_task']=asyncio.create_task(run_backtest(pct))
    return web.json_response({'running':True})
async def bt_status(request):
    _,s=get_session(request)
    if not s:return web.json_response({'error':'not_logged_in'},status=401)
    return web.json_response(BT)
async def bt_download(request):
    _,s=get_session(request)
    if not s:return web.json_response({'error':'not_logged_in'},status=401)
    if not BT.get('done') or not BT.get('results'):return web.json_response({'error':'backtest_not_ready'},status=409)
    return web.Response(text=json.dumps(BT['results'],ensure_ascii=False,indent=2),content_type='application/json',headers={'Content-Disposition':'attachment; filename="ADONIS_BACKTEST.json"','Cache-Control':'no-store'})

async def api_exec_arm(request):
    _,s=get_session(request)
    if not s:return web.json_response({'error':'not_logged_in'},status=401)
    try:payload=await request.json()
    except Exception:payload={}
    account=str(payload.get('account_id',''))
    if not ACCOUNT_RE.match(account):return web.json_response({'error':'account_id invalide'},status=400)
    try:
        async with aiohttp.ClientSession(timeout=TIMEOUT) as http:
            async with http.get(API_BASE+'/trading/v1/options/accounts',headers=deriv_headers(s['token'])) as r: raw=await r.text()
            acc=next((a for a in account_list(parse_json(raw)) if isinstance(a,dict) and account_id_of(a)==account),None)
            currency=str(acc.get('currency','')).strip() if acc else ''
            if not currency:return web.json_response({'error':'devise du compte introuvable dans la réponse Deriv'},status=422)
            async with http.post(API_BASE+'/trading/v1/options/accounts/'+account+'/otp',headers=deriv_headers(s['token'])) as r: st=r.status; raw2=await r.text()
    except Exception as exc:return web.json_response({'error':'network','detail':str(exc)},status=502)
    body=parse_json(raw2); url=body.get('data',{}).get('url') if isinstance(body,dict) else None
    if not (200<=st<300 and url):return web.json_response({'error':'otp_failed','status':st},status=502)
    if not is_demo_url(url):return web.json_response({'error':'Compte réel refusé : seul un compte DÉMO peut être armé.'},status=403)
    EXEC.update(account_id=account,currency=currency,token=s['token'],exp=s['exp'],armed_at=time.time())
    return web.json_response({'armed':True,'account_id':account})
async def api_exec_disarm(request):
    _,s=get_session(request)
    if not s:return web.json_response({'error':'not_logged_in'},status=401)
    EXEC.update(account_id=None,currency=None,token=None,exp=0,armed_at=None);PENDING_ORDERS.clear()
    return web.json_response({'armed':False})
async def api_exec_status(request):
    _,s=get_session(request)
    if not s:return web.json_response({'error':'not_logged_in'},status=401)
    ok=bool(EXEC['account_id'] and EXEC['exp']>time.time())
    return web.json_response({'armed':ok,'account_id':EXEC['account_id'] if ok else None,'expires':EXEC['exp'] if ok else None,'risk':RISK_PER_TRADE,'ttl_min':EXEC_TTL//60})
async def api_exec_run(request):
    # Appelé par le bouton ntfy (POST). Pas de cookie : le jeton à usage unique est la protection.
    tok=request.match_info.get('tok','')
    p=PENDING_ORDERS.get(tok)
    if not p or p['used'] or p['exp']<time.time():
        return web.json_response({'ok':False,'msg':'Lien expiré ou déjà utilisé.'},status=410)
    p['used']=True
    res=await execute_order(p)
    await send_notify('ADONIS — '+('ordre exécuté' if res['ok'] else 'ordre NON exécuté'),p['name']+' : '+res['msg'])
    return web.json_response(res,status=200 if res['ok'] else 422)
async def startup(app):app['scanner_task']=asyncio.create_task(scanner_loop(app))
async def cleanup(app):
    for key in ('scanner_task','bt_task'):
        task=app.get(key)
        if task and not task.done():task.cancel()
app=web.Application(client_max_size=2**20)
app.router.add_get('/',index);app.router.add_get('/login',login);app.router.add_get('/callback',callback);app.router.add_get('/logout',logout)
app.router.add_get('/api/me',api_me);app.router.add_get('/api/accounts',api_accounts);app.router.add_post('/api/otp',api_otp)
app.router.add_get('/api/scanner',api_scanner);app.router.add_post('/api/backtest/start',start_bt);app.router.add_get('/api/backtest',bt_status);app.router.add_get('/api/backtest/download',bt_download)
app.router.add_post('/api/exec/arm',api_exec_arm);app.router.add_post('/api/exec/disarm',api_exec_disarm);app.router.add_get('/api/exec/status',api_exec_status);app.router.add_post('/api/exec/run/{tok}',api_exec_run)
app.on_startup.append(startup);app.on_cleanup.append(cleanup)
if __name__=='__main__':
    print('ADONIS CRT H4 | trend filter:',TREND_FILTER,'| app id:',bool(APP_ID),'| markets:',len(SYMBOLS),'| Notify:',bool(NTFY_TOPIC),'| redirect:',REDIRECT_URI,flush=True)
    web.run_app(app,host='0.0.0.0',port=PORT)
