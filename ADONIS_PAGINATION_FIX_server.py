import os, json, time, math, asyncio, re, base64, hashlib, secrets, html
from urllib.parse import urlencode
from collections import deque
from datetime import datetime, timezone
import aiohttp
from aiohttp import web

# ================================================================
# ADONIS — BASE RECONSTRUITE
# Seuls les éléments d'interface conservés : Notify, Backtest, JSON.
# Moteur neuf : biais H4 -> CRT H4 -> MSS M15 -> FVG/OB -> retest
# distinctement confirmé. Aucun ordre n'est envoyé.
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
BT_DAYS = 180
SCAN_SECONDS = max(30, int(os.getenv('SCAN_EVERY', '60')))
SYMBOLS = [
    ('frxXAUUSD','XAUUSD'), ('cryBTCUSD','BTCUSD'), ('frxEURUSD','EURUSD'),
    ('frxUSDJPY','USDJPY'), ('frxAUDCAD','AUDCAD'), ('1HZ50V','Volatility 50 (1s)'),
    ('BOOM500','Boom 500'), ('CRASH500','Crash 500'), ('CRASH1000','Crash 1000'),
    ('BOOM1000','Boom 1000'), ('JD10','Jump 10'), ('stpRNG','Step Index')
]

STATE = {'scanner':'initialisation','last_scan':None,'last_error':None,'last_signal':None}
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
    $('accountCard').style.display='block';$('btCard').style.display='block';$('scanCard').style.display='block';$('radarCard').style.display='block';
    await loadAccounts();await loadBacktest();await loadScanner();setInterval(loadScanner,30000);
  }catch(e){log('Erreur serveur : '+e);}
}
async function loadAccounts(){try{const r=await fetch('/api/accounts');const j=await r.json();$('debug').textContent=JSON.stringify(j,null,2);const body=j.body||{};const list=Array.isArray(body)?body:(body.data||body.accounts||[]);const sel=$('accountSelect');sel.innerHTML='';list.forEach(a=>{const id=a.account_id||a.accountId||a.id||a.loginid;if(!id)return;const o=document.createElement('option');o.value=id;o.textContent=[id,a.account_type||a.type||'',a.balance!==undefined?(a.balance+' '+(a.currency||'')):''].filter(Boolean).join(' · ');sel.appendChild(o)});if(!sel.options.length)log('Aucun compte reconnu dans la réponse Deriv.');else log(sel.options.length+' compte(s) Deriv trouvé(s).');}catch(e){log('Chargement comptes : '+e)}}
async function connectWs(){const id=$('accountSelect').value;if(!id){log('Choisis un compte.');return;}closeWs();badge('Connexion…','#eab308');try{const r=await fetch('/api/otp',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({account_id:id})});const j=await r.json();if(!j.url)throw new Error(JSON.stringify(j));ws=new WebSocket(j.url);ws.onopen=()=>{badge('Connecté (Live)','#22c55e');$('brokerDisplay').textContent='Deriv — '+id;log('WebSocket Deriv connecté.');send({balance:1,subscribe:1});send({active_symbols:'brief'});subscribeTicker();};ws.onmessage=e=>{let d;try{d=JSON.parse(e.data)}catch(_){return}if(d.error){log('Deriv : '+d.error.message);return;}if(d.msg_type==='balance'&&d.balance){$('accountBalance').textContent=d.balance.balance+' '+d.balance.currency;}if(d.msg_type==='tick'&&d.tick){$('livePrice').textContent=d.tick.quote;}};ws.onclose=()=>{badge('Déconnecté','#eab308');log('WebSocket Deriv fermé.');};ws.onerror=()=>log('Erreur WebSocket Deriv.');}catch(e){badge('Erreur','#ef4444');log('Connexion Deriv : '+e.message);}}
function closeWs(){if(ws){try{ws.close()}catch(e){}ws=null;}badge('Déconnecté','#eab308');$('brokerDisplay').textContent='Non connecté';$('accountBalance').textContent='--';}
async function loadScanner(){try{const j=await(await fetch('/api/scanner')).json();$('scanMeta').textContent='Dernière analyse : '+(j.derniere_analyse?new Date(j.derniere_analyse*1000).toLocaleTimeString():'en attente')+' · Notify : '+(j.ntfy_etat||'');$('scanList').innerHTML='';(j.signaux||[]).forEach(s=>{let d=document.createElement('div');d.className='log-item';d.textContent='🔔 '+s; $('scanList').appendChild(d)});Object.entries(j.symboles||{}).forEach(([s,v])=>{let d=document.createElement('div');d.className='log-item';d.textContent=(v.label||s)+' — '+v.resume;$('scanList').appendChild(d)});$('scanSymbols').textContent=(j.disponibles||[]).map(x=>x[0]+' = '+x[1]).join('\\n');}catch(e){}}
async function startBacktest(){$('btBtn').disabled=true;$('btStatus').textContent='Démarrage du backtest…';$('btDownload').style.display='none';$('btList').innerHTML='';try{const r=await fetch('/api/backtest/start',{method:'POST'});const j=await r.json();if(!r.ok)throw new Error(j.error||r.status);if(btTimer)clearInterval(btTimer);btTimer=setInterval(loadBacktest,2500);loadBacktest();}catch(e){$('btStatus').textContent='Erreur : '+e;$('btBtn').disabled=false;}}
const fmt=x=>typeof x==='number'?(x>0?'+':'')+x.toFixed(3):x;
async function loadBacktest(){try{const r=await fetch('/api/backtest');if(r.status===401)return;const j=await r.json();$('btStatus').textContent=j.error?'Erreur : '+j.error:(j.running?'En cours : '+j.progress:(j.done?'Backtest terminé.':'Prêt.'));$('btDownload').style.display=(j.done&&j.results)?'block':'none';if(!j.running){$('btBtn').disabled=false;if(btTimer){clearInterval(btTimer);btTimer=null;}}if(!j.results)return;const p=j.results.portfolio||{};$('btList').innerHTML='';let head=document.createElement('div');head.className='log-item';head.textContent=`PORTFEUILLE : ${p.trades} trades · ${p.wins} gagnés · ${p.losses} perdus · ${fmt(p.r_total)} R · PF ${p.pf} · DD ${p.max_dd_r} R`; $('btList').appendChild(head);(j.results.markets||[]).forEach(x=>{let d=document.createElement('div');d.className='log-item';d.textContent=x.name+' : '+(x.error?'ERREUR — '+x.error:`${x.trades} trades · ${fmt(x.r_total)} R · PF ${x.pf} · DD ${x.max_dd_r} R`);$('btList').appendChild(d)});if(j.results.incomplete_markets&&j.results.incomplete_markets.length){let d=document.createElement('div');d.className='small';d.textContent='Attention : backtest partiel, marchés en erreur : '+j.results.incomplete_markets.join(', ');$('btList').appendChild(d);}}catch(e){$('btStatus').textContent='Erreur de lecture : '+e;}}
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
    last = None
    for attempt in range(retries):
        try:
            if ws_holder[0] is None or ws_holder[0].closed:
                ws_holder[0] = await connect_ws(session)
            return await request(ws_holder[0], payload)
        except (ConnectionError, aiohttp.ClientError, asyncio.TimeoutError, OSError, RuntimeError) as exc:
            last = exc
            old = ws_holder[0]
            ws_holder[0] = None
            if old is not None:
                try: await old.close()
                except Exception: pass
            if attempt + 1 < retries:
                await asyncio.sleep(min(8, 1.5 * (attempt + 1)))
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
    # FX closes on weekends; use a conservative coverage floor, and expose counts.
    min_required = int(expected * (0.42 if granularity == 900 else 0.50))
    if len(result) < min_required:
        raise DerivError(f'historique incomplet : {len(result)}/{expected} bougies attendues')
    return result

async def fetch_latest(session, ws_holder, symbol, granularity, count):
    resp = await request_resilient(session, ws_holder, {'ticks_history': symbol, 'end': 'latest', 'style': 'candles', 'granularity': granularity, 'count': count, 'adjust_start_time': 1})
    now = int(time.time())
    return [c for c in normalize(resp.get('candles')) if c['t'] + granularity <= now]


def atr(candles, i, period=14):
    if i<period: return None
    vals=[]
    for k in range(i-period+1,i+1):
        p=candles[k-1]['c']; x=candles[k]
        vals.append(max(x['h']-x['l'],abs(x['h']-p),abs(x['l']-p)))
    return sum(vals)/len(vals) if vals else None

def pivots(candles, left=2, right=2):
    highs=[]; lows=[]
    for i in range(left,len(candles)-right):
        h=candles[i]['h']; lo=candles[i]['l']
        if all(h>candles[j]['h'] for j in range(i-left,i)) and all(h>=candles[j]['h'] for j in range(i+1,i+right+1)): highs.append((i,h))
        if all(lo<candles[j]['l'] for j in range(i-left,i)) and all(lo<=candles[j]['l'] for j in range(i+1,i+right+1)): lows.append((i,lo))
    return highs,lows

def h4_bias(h4, at_time=None):
    cs=[c for c in h4 if at_time is None or c['t']+14400<=at_time]
    hs,ls=pivots(cs)
    if len(hs)<2 or len(ls)<2:return None
    hh=hs[-1][1]>hs[-2][1]; hl=ls[-1][1]>ls[-2][1]
    lh=hs[-1][1]<hs[-2][1]; ll=ls[-1][1]<ls[-2][1]
    if hh and hl:return 'BUY'
    if lh and ll:return 'SELL'
    return None

def find_latest_crt(h4, bias, after_time=None):
    # CRT = bougie 2 balaie un côté de la range bougie 1 et clôture dans cette range.
    for i in range(len(h4)-1,0,-1):
        a,b=h4[i-1],h4[i]
        if after_time is not None and b['t']+14400>after_time: continue
        later=[x for x in h4[i+1:] if after_time is None or x['t']+14400<=after_time]
        if bias=='BUY' and b['l']<a['l'] and a['l']<b['c']<a['h']:
            # Invalidation : une clôture H4 ultérieure au-dessus de l'autre bord du CRT.
            if any(x['c']>a['h'] for x in later): continue
            return {'side':'BUY','time':b['t']+14400,'high':a['h'],'low':a['l'],'sweep':b['l']}
        if bias=='SELL' and b['h']>a['h'] and a['l']<b['c']<a['h']:
            # Invalidation miroir : clôture H4 ultérieure sous l'autre bord du CRT.
            if any(x['c']<a['l'] for x in later): continue
            return {'side':'SELL','time':b['t']+14400,'high':a['h'],'low':a['l'],'sweep':b['h']}
    return None

def fvg_at(m15, i, side):
    if i<2:return None
    a,b,c=m15[i-2],m15[i-1],m15[i]
    if side=='BUY' and c['l']>a['h']:
        return {'kind':'FVG','lo':a['h'],'hi':c['l'],'formed':c['t'],'impulse':i}
    if side=='SELL' and c['h']<a['l']:
        return {'kind':'FVG','lo':c['h'],'hi':a['l'],'formed':c['t'],'impulse':i}
    return None

def detect_setup(h4,m15,at_index):
    now=m15[at_index]['t']+900
    bias=h4_bias(h4,now)
    if not bias:return None
    crt=find_latest_crt(h4,bias,now)
    if not crt or crt['time']>=now:return None
    # Les swings M15 sont calculés uniquement sur les bougies clôturées avant le point courant.
    prior=m15[:at_index]
    sh,sl=pivots(prior)
    if bias=='BUY':
        refs=[x for x in sh if prior[x[0]]['t']>=crt['time']]
        if not refs:return None
        ref_i,ref=refs[-1]
        mss_i=None
        for k in range(ref_i+3,at_index+1):
            if m15[k]['c']>ref: mss_i=k;break
        if mss_i is None:return None
    else:
        refs=[x for x in sl if prior[x[0]]['t']>=crt['time']]
        if not refs:return None
        ref_i,ref=refs[-1]
        mss_i=None
        for k in range(ref_i+3,at_index+1):
            if m15[k]['c']<ref: mss_i=k;break
        if mss_i is None:return None
    # Après le MSS, on suit les FVG et le dernier OB opposé simultanément.
    zones=[]
    for k in range(mss_i,at_index+1):
        z=fvg_at(m15,k,bias)
        if z: zones.append(z)
    # OB provisoire validé : dernière bougie opposée avant l'impulsion de MSS.
    ob=None
    for k in range(mss_i-1,max(-1,mss_i-12),-1):
        candle=m15[k]
        if (bias=='BUY' and candle['c']<candle['o']) or (bias=='SELL' and candle['c']>candle['o']):
            ob={'kind':'OB','lo':candle['l'],'hi':candle['h'],'formed':candle['t'],'impulse':mss_i};break
    if ob: zones.append(ob)
    # La zone la plus récente est surveillée en premier; le test et la confirmation doivent être deux bougies distinctes.
    zones.sort(key=lambda z:z['formed'],reverse=True)
    for z in zones:
        if z['formed']>=m15[at_index]['t']: continue
        test=None
        for k in range(z['impulse']+1,at_index):
            c=m15[k]
            touched=(c['l']<=z['hi'] and c['h']>=z['lo'])
            if touched: test=k;break
        if test is None or test>=at_index:continue
        confirm=m15[at_index]
        if bias=='BUY' and confirm['c']>z['hi'] and confirm['c']>confirm['o']:
            a=atr(m15,test) or (m15[test]['h']-m15[test]['l'])
            entry=confirm['c']; sl=m15[test]['l']-0.1*a
            confirmed_highs=[p for ix,p in pivots(h4)[0] if p>entry and ix+2<len(h4) and h4[ix+2]['t']+14400<=now]
            tp=min(confirmed_highs) if confirmed_highs else None
            if tp and entry>sl:return {'side':bias,'entry':entry,'sl':sl,'tp':tp,'zone':z['kind'],'zone_lo':z['lo'],'zone_hi':z['hi'],'test_time':m15[test]['t'],'confirm_time':confirm['t'],'mss_time':m15[mss_i]['t'],'crt_time':crt['time']}
        if bias=='SELL' and confirm['c']<z['lo'] and confirm['c']<confirm['o']:
            a=atr(m15,test) or (m15[test]['h']-m15[test]['l'])
            entry=confirm['c']; sl=m15[test]['h']+0.1*a
            confirmed_lows=[p for ix,p in pivots(h4)[1] if p<entry and ix+2<len(h4) and h4[ix+2]['t']+14400<=now]
            tp=max(confirmed_lows) if confirmed_lows else None
            if tp and sl>entry:return {'side':bias,'entry':entry,'sl':sl,'tp':tp,'zone':z['kind'],'zone_lo':z['lo'],'zone_hi':z['hi'],'test_time':m15[test]['t'],'confirm_time':confirm['t'],'mss_time':m15[mss_i]['t'],'crt_time':crt['time']}
    return None

def scan_closed(h4,m15):
    if len(m15)<100 or len(h4)<30:return None
    # On teste uniquement la dernière bougie M15 clôturée.
    return detect_setup(h4,m15,len(m15)-1)

async def send_notify(title, message):
    if not NTFY_URL:return False
    try:
        timeout=aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post(NTFY_URL,data=message.encode(),headers={'Title':title,'Priority':'high'}) as r:
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
                        sig = scan_closed(h4, m15)
                        if sig:
                            key = (sym, sig['side'], sig['confirm_time'], sig['zone'])
                            if key not in _SENT:
                                _SENT.add(key)
                                msg = (f"{name} {sig['side']} | {sig['zone']} | entrée {sig['entry']} | SL {sig['sl']} | TP {sig['tp']}")
                                SIGNALS.appendleft(msg); STATE['last_signal'] = msg
                                await send_notify('ADONIS — signal', msg)
                        STATE['symbols'][sym] = {'label': name, 'resume': 'analyse terminée'}
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


def simulate_trades(m15, start_index, signal):
    side=signal['side']; entry=signal['entry']; sl=signal['sl']; tp=signal['tp']
    risk=abs(entry-sl)
    if risk<=0:return None
    trail_zones=[]
    used_tests=set()
    for k in range(start_index+1,len(m15)):
        c=m15[k]
        # L'ancien SL reste actif sur toute la bougie; SL prioritaire si SL et TP sont touchés.
        if side=='BUY':
            if c['l']<=sl:return {'r':(sl-entry)/risk,'outcome':'SL/TRAIL','exit_time':c['t']}
            if c['h']>=tp:return {'r':(tp-entry)/risk,'outcome':'TP','exit_time':c['t']}
        else:
            if c['h']>=sl:return {'r':(entry-sl)/risk,'outcome':'SL/TRAIL','exit_time':c['t']}
            if c['l']<=tp:return {'r':(entry-tp)/risk,'outcome':'TP','exit_time':c['t']}
        # Un FVG nouveau devient candidat au trailing, jamais sur la bougie qui le crée.
        z=fvg_at(m15,k,side)
        if z: trail_zones.append(z)
        # Retest sur une bougie, confirmation sur la suivante; le nouveau SL prend effet à k+1.
        test=m15[k-1]
        for zi,z in enumerate(trail_zones):
            if zi in used_tests or z['formed']>=test['t']: continue
            touched=test['l']<=z['hi'] and test['h']>=z['lo']
            confirmed=(c['c']>z['hi'] and c['c']>c['o']) if side=='BUY' else (c['c']<z['lo'] and c['c']<c['o'])
            if touched and confirmed:
                a=atr(m15,k-1) or (test['h']-test['l'])
                candidate=(test['l']-0.1*a) if side=='BUY' else (test['h']+0.1*a)
                if side=='BUY' and sl<candidate<tp: sl=candidate
                if side=='SELL' and tp<candidate<sl: sl=candidate
                used_tests.add(zi)
    return {'r':0.0,'outcome':'OPEN','exit_time':m15[-1]['t'],'final_sl':sl}

def stats(trades):
    vals=[x['r'] for x in trades]; wins=sum(x for x in vals if x>0); losses=-sum(x for x in vals if x<0)
    eq=peak=dd=0.0
    for x in vals:
        eq+=x;peak=max(peak,eq);dd=max(dd,peak-eq)
    return {'trades':len(vals),'wins':sum(x>0 for x in vals),'losses':sum(x<0 for x in vals),'open':sum(x==0 for x in vals),'r_total':round(sum(vals),3),'expectancy_r':round(sum(vals)/len(vals),4) if vals else 0,'pf':round(wins/losses,3) if losses else ('inf' if wins else 0),'max_dd_r':round(dd,3)}

async def run_backtest():
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
                    # New connection per market; every page request can reconnect transparently.
                    h4 = await fetch_history(session, holder, sym, 14400, start - 45*86400, end)
                    m15 = await fetch_history(session, holder, sym, 900, start, end)
                    trades, last_exit = [], 0
                    for i in range(100, len(m15)-1):
                        if i % 500 == 0:
                            BT['progress'] = f'{n}/12 — {name} : replay {i}/{len(m15)}'
                        if m15[i]['t'] < start or m15[i]['t'] <= last_exit:
                            continue
                        local_start = max(0, i-700)
                        local_m15 = m15[local_start:i+1]
                        sig = detect_setup(h4, local_m15, len(local_m15)-1)
                        if not sig:
                            continue
                        sim = simulate_trades(m15, i, sig)
                        if sim:
                            trade = {**sim, 'symbol':sym, 'name':name, 'side':sig['side'], 'zone':sig['zone'], 'entry':sig['entry'], 'sl':sig['sl'], 'tp':sig['tp'], 'signal_time':m15[i]['t']}
                            trades.append(trade); all_trades.append(trade); last_exit = sim['exit_time']
                    markets.append({'symbol':sym, 'name':name, **stats(trades), 'error':None,
                                    'h4_candles':len(h4), 'm15_candles':len(m15)})
                except Exception as exc:
                    print(f'[BACKTEST] {name} error: {type(exc).__name__}: {exc}', flush=True)
                    markets.append({'symbol':sym, 'name':name, 'error':f'{type(exc).__name__}: {str(exc)[:200]}', **stats([])})
                finally:
                    if holder[0] is not None:
                        try: await holder[0].close()
                        except Exception: pass
                await asyncio.sleep(1.0)
        portfolio = stats(sorted(all_trades, key=lambda x:(x['exit_time'],x['signal_time'])))
        incomplete = [x['name'] for x in markets if x.get('error')]
        BT['results'] = {
            'generated_at':datetime.now(timezone.utc).isoformat(), 'period_days':BT_DAYS,
            'window_start':start, 'window_end':end,
            'strategy':'ADONIS RESET — H4 structure + CRT + M15 MSS + FVG/OB retest-confirmation',
            'status':'PARTIAL' if incomplete else 'COMPLETE',
            'markets_total':len(SYMBOLS), 'markets_success':len(SYMBOLS)-len(incomplete),
            'incomplete_markets':incomplete, 'markets':markets, 'portfolio':portfolio,
            'trades':all_trades,
            'notes':['Aucun ordre réel.', 'Coûts, spread et slippage non inclus.',
                     'Un résultat PARTIAL ne doit pas être interprété comme un backtest complet.',
                     'Données téléchargées marché par marché avec reconnexion du WebSocket en cas de fermeture.']
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
    if not BT['running']:request.app['bt_task']=asyncio.create_task(run_backtest())
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
async def startup(app):app['scanner_task']=asyncio.create_task(scanner_loop(app))
async def cleanup(app):
    for key in ('scanner_task','bt_task'):
        task=app.get(key)
        if task and not task.done():task.cancel()
app=web.Application(client_max_size=2**20)
app.router.add_get('/',index);app.router.add_get('/login',login);app.router.add_get('/callback',callback);app.router.add_get('/logout',logout)
app.router.add_get('/api/me',api_me);app.router.add_get('/api/accounts',api_accounts);app.router.add_post('/api/otp',api_otp)
app.router.add_get('/api/scanner',api_scanner);app.router.add_post('/api/backtest/start',start_bt);app.router.add_get('/api/backtest',bt_status);app.router.add_get('/api/backtest/download',bt_download)
app.on_startup.append(startup);app.on_cleanup.append(cleanup)
if __name__=='__main__':
    print('ADONIS repair | app id:',bool(APP_ID),'| markets:',len(SYMBOLS),'| Notify:',bool(NTFY_TOPIC),'| redirect:',REDIRECT_URI,flush=True)
    web.run_app(app,host='0.0.0.0',port=PORT)
