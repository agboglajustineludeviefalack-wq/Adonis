import os, json, time, math, asyncio
from collections import deque
from datetime import datetime, timezone
import aiohttp
from aiohttp import web

# ================================================================
# ADONIS — BASE RECONSTRUITE
# Notify, Backtest et téléchargement JSON.
# Moteur : biais H4 -> CRT H4 -> MSS M15 -> FVG/OB -> retest confirmé.
# Aucun ordre réel n'est envoyé.
# ================================================================

APP_ID = os.getenv('DERIV_APP_ID', '').strip()
PUBLIC_WS = os.getenv(
    'DERIV_PUBLIC_WS',
    'wss://api.derivws.com/trading/v1/options/ws/public'
)
NTFY_TOPIC = os.getenv('NTFY_TOPIC', '').strip()
NTFY_URL = 'https://ntfy.sh/' + NTFY_TOPIC if NTFY_TOPIC else ''
PORT = int(os.getenv('PORT', '8080'))
BT_DAYS = 180
SCAN_SECONDS = max(30, int(os.getenv('SCAN_EVERY', '60')))

SYMBOLS = [
    ('frxXAUUSD', 'XAUUSD'),
    ('cryBTCUSD', 'BTCUSD'),
    ('frxEURUSD', 'EURUSD'),
    ('frxUSDJPY', 'USDJPY'),
    ('frxAUDCAD', 'AUDCAD'),
    ('1HZ50V', 'Volatility 50 (1s)'),
    ('BOOM500', 'Boom 500'),
    ('CRASH500', 'Crash 500'),
    ('CRASH1000', 'Crash 1000'),
    ('BOOM1000', 'Boom 1000'),
    ('JD10', 'Jump 10'),
    ('stpRNG', 'Step Index')
]

STATE = {
    'scanner': 'initialisation',
    'last_scan': None,
    'last_error': None,
    'last_signal': None
}

BT = {
    'running': False,
    'done': False,
    'progress': '',
    'error': None,
    'results': None
}

SIGNALS = deque(maxlen=100)
_SENT = set()
_REQ_ID = 0

HTML = r'''<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ADONIS</title>
<style>
body{margin:0;padding:18px;background:#0d1117;color:#e6edf3;font:15px system-ui}
main{max-width:760px;margin:auto}
.panel{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:18px;margin:14px 0}
h1{font-size:23px;color:#58a6ff}
h2{font-size:17px;margin:0 0 12px}
.muted{color:#8b949e;font-size:13px}
.btn{display:block;width:100%;padding:13px;border:0;border-radius:8px;background:#2563eb;color:white;font-weight:700;font-size:15px;margin-top:10px}
.btn:disabled{opacity:.5}
.download{color:#79c0ff;margin-top:14px;display:none}
.line{padding:10px;background:#0d1117;border-radius:8px;margin:7px 0;white-space:pre-wrap;font-size:13px}
.status{color:#3fb950}
</style>
</head>
<body>
<main>
<h1>ADONIS</h1>

<section class="panel">
<h2>Notify</h2>
<div id="notify" class="muted">
Vérification du service de notifications…
</div>
<div id="lastSignal" class="line" style="display:none"></div>
</section>

<section class="panel">
<h2>Backtest historique — 180 jours</h2>
<p class="muted">
Test des règles ADONIS sur les 12 marchés.
Aucun ordre réel n'est exécuté.
</p>
<button id="run" class="btn" onclick="startBT()">
Lancer le backtest
</button>
<div id="progress" class="muted" style="margin-top:12px"></div>
<a id="download" class="download" href="/api/backtest/download">
Télécharger le JSON
</a>
<div id="results"></div>
</section>
</main>

<script>
const $=x=>document.getElementById(x);
let timer=null;

async function status(){
    try{
        let r=await fetch('/api/status');
        let j=await r.json();
        $('notify').textContent=
            j.notify+(j.error?' — '+j.error:'');
        $('notify').className=j.notify_ok?'status':'muted';

        if(j.last_signal){
            $('lastSignal').style.display='block';
            $('lastSignal').textContent=j.last_signal;
        }
    }catch(e){
        $('notify').textContent='Serveur indisponible';
    }
}

async function startBT(){
    $('run').disabled=true;
    $('progress').textContent='Démarrage…';
    $('results').innerHTML='';
    $('download').style.display='none';

    try{
        await fetch('/api/backtest/start',{method:'POST'});
        if(timer)clearInterval(timer);
        timer=setInterval(loadBT,2500);
        loadBT();
    }catch(e){
        $('progress').textContent='Erreur : '+e;
        $('run').disabled=false;
    }
}

async function loadBT(){
    try{
        let j=await(await fetch('/api/backtest')).json();

        $('progress').textContent=
            j.error?'Erreur : '+j.error:
            (j.running?'En cours : '+j.progress:
            (j.done?'Terminé.':''));

        if(j.done&&j.results){
            $('download').style.display='inline-block';

            $('results').innerHTML=
                '<div class="line">'+
                JSON.stringify(j.results.portfolio,null,2)
                    .replaceAll('<','&lt;')+
                '</div>'+
                '<div class="line">'+
                j.results.markets.map(x=>
                    x.name+': '+
                    (x.error||(
                        `${x.trades} trades · ${x.r_total} R · PF ${x.pf}`
                    ))
                ).join('\n')+
                '</div>';
        }

        if(!j.running){
            $('run').disabled=false;
            if(timer){
                clearInterval(timer);
                timer=null;
            }
        }
    }catch(e){
        $('progress').textContent='Erreur de lecture';
    }
}

status();
setInterval(status,10000);
loadBT();
</script>
</body>
</html>'''

class DerivError(Exception):
    pass


async def connect_ws(session):
    return await session.ws_connect(
        PUBLIC_WS,
        heartbeat=25,
        timeout=30
    )


async def request(ws, payload, timeout=30):
    global _REQ_ID

    _REQ_ID += 1
    rid = _REQ_ID

    data = dict(payload)
    data['req_id'] = rid

    await ws.send_str(json.dumps(data))

    end = time.monotonic() + timeout

    while time.monotonic() < end:
        msg = await ws.receive(
            timeout=max(1, end-time.monotonic())
        )

        if msg.type == aiohttp.WSMsgType.TEXT:
            try:
                obj = json.loads(msg.data)
            except Exception:
                continue

            if obj.get('req_id') == rid:
                if obj.get('error'):
                    raise DerivError(
                        obj['error'].get('message', 'Erreur Deriv')
                    )
                return obj

        elif msg.type in (
            aiohttp.WSMsgType.CLOSED,
            aiohttp.WSMsgType.ERROR,
            aiohttp.WSMsgType.CLOSE
        ):
            raise ConnectionError('WebSocket Deriv fermé')

    raise asyncio.TimeoutError('Délai Deriv dépassé')


def normalize(raw):
    out = []

    for c in raw or []:
        try:
            x = {
                't': int(c.get('epoch', c.get('t'))),
                'o': float(c.get('open', c.get('o'))),
                'h': float(c.get('high', c.get('h'))),
                'l': float(c.get('low', c.get('l'))),
                'c': float(c.get('close', c.get('c')))
            }

            if (
                all(math.isfinite(x[k]) for k in ('o','h','l','c'))
                and x['l'] <= min(x['o'],x['c'])
                <= max(x['o'],x['c']) <= x['h']
            ):
                out.append(x)

        except Exception:
            pass

    out.sort(key=lambda x: x['t'])
    dedup = {x['t']: x for x in out}

    return [dedup[t] for t in sorted(dedup)]


async def fetch_history(
    ws, symbol, granularity, start, end, page_size=5000
):
    """Téléchargement paginé borné par une fenêtre commune."""
    cursor_end = end
    allc = {}
    pages = 0

    while cursor_end > start and pages < 30:
        resp = await request(ws, {
            'ticks_history': symbol,
            'end': cursor_end,
            'style': 'candles',
            'granularity': granularity,
            'count': min(page_size, 1000)
        })

        batch = normalize(resp.get('candles'))

        if not batch:
            break

        for c in batch:
            close_t = c['t'] + granularity

            if c['t'] >= start and close_t <= end:
                allc[c['t']] = c

        oldest = min(x['t'] for x in batch)

        if oldest <= start or oldest >= cursor_end:
            break

        cursor_end = oldest - 1
        pages += 1

        if len(batch) < 2:
            break

        await asyncio.sleep(.35)

    result = [allc[t] for t in sorted(allc)]
    expected = max(0, int((end-start)//granularity))

    min_required = (
        int(expected*.55)
        if granularity == 900
        else int(expected*.60)
    )

    if len(result) < min_required:
        raise DerivError(
            f'historique insuffisant {len(result)}/{expected} bougies'
        )

    return result


def atr(candles, i, period=14):
    if i < period:
        return None

    vals = []

    for k in range(i-period+1, i+1):
        p = candles[k-1]['c']
        x = candles[k]

        vals.append(max(
            x['h']-x['l'],
            abs(x['h']-p),
            abs(x['l']-p)
        ))

    return sum(vals)/len(vals) if vals else None


def pivots(candles, left=2, right=2):
    highs = []
    lows = []

    for i in range(left, len(candles)-right):
        h = candles[i]['h']
        lo = candles[i]['l']

        if (
            all(h > candles[j]['h'] for j in range(i-left,i))
            and all(h >= candles[j]['h'] for j in range(i+1,i+right+1))
        ):
            highs.append((i,h))

        if (
            all(lo < candles[j]['l'] for j in range(i-left,i))
            and all(lo <= candles[j]['l'] for j in range(i+1,i+right+1))
        ):
            lows.append((i,lo))

    return highs, lows


def h4_bias(h4, at_time=None):
    cs = [
        c for c in h4
        if at_time is None or c['t']+14400 <= at_time
    ]

    hs, ls = pivots(cs)

    if len(hs) < 2 or len(ls) < 2:
        return None

    hh = hs[-1][1] > hs[-2][1]
    hl = ls[-1][1] > ls[-2][1]

    lh = hs[-1][1] < hs[-2][1]
    ll = ls[-1][1] < ls[-2][1]

    if hh and hl:
        return 'BUY'

    if lh and ll:
        return 'SELL'

    return None


def find_latest_crt(h4, bias, after_time=None):
    # CRT : bougie 2 balaie un côté de la range bougie 1
    # et clôture à l'intérieur de cette range.

    for i in range(len(h4)-1, 0, -1):
        a, b = h4[i-1], h4[i]

        if after_time is not None and b['t']+14400 > after_time:
            continue

        later = [
            x for x in h4[i+1:]
            if after_time is None or x['t']+14400 <= after_time
        ]

        if (
            bias == 'BUY'
            and b['l'] < a['l']
            and a['l'] < b['c'] < a['h']
        ):
            if any(x['c'] > a['h'] for x in later):
                continue

            return {
                'side': 'BUY',
                'time': b['t']+14400,
                'high': a['h'],
                'low': a['l'],
                'sweep': b['l']
            }

        if (
            bias == 'SELL'
            and b['h'] > a['h']
            and a['l'] < b['c'] < a['h']
        ):
            if any(x['c'] < a['l'] for x in later):
                continue

            return {
                'side': 'SELL',
                'time': b['t']+14400,
                'high': a['h'],
                'low': a['l'],
                'sweep': b['h']
            }

    return None


def fvg_at(m15, i, side):
    if i < 2:
        return None

    a, b, c = m15[i-2], m15[i-1], m15[i]

    if side == 'BUY' and c['l'] > a['h']:
        return {
            'kind': 'FVG',
            'lo': a['h'],
            'hi': c['l'],
            'formed': c['t'],
            'impulse': i
        }

    if side == 'SELL' and c['h'] < a['l']:
        return {
            'kind': 'FVG',
            'lo': c['h'],
            'hi': a['l'],
            'formed': c['t'],
            'impulse': i
        }

    return None


def detect_setup(h4, m15, at_index):
    now = m15[at_index]['t'] + 900

    bias = h4_bias(h4, now)

    if not bias:
        return None

    crt = find_latest_crt(h4, bias, now)

    if not crt or crt['time'] >= now:
        return None

    prior = m15[:at_index]
    sh, sl = pivots(prior)

    if bias == 'BUY':
        refs = [
            x for x in sh
            if prior[x[0]]['t'] >= crt['time']
        ]

        if not refs:
            return None

        ref_i, ref = refs[-1]
        mss_i = None

        for k in range(ref_i+3, at_index+1):
            if m15[k]['c'] > ref:
                mss_i = k
                break

        if mss_i is None:
            return None

    else:
        refs = [
            x for x in sl
            if prior[x[0]]['t'] >= crt['time']
        ]

        if not refs:
            return None

        ref_i, ref = refs[-1]
        mss_i = None

        for k in range(ref_i+3, at_index+1):
            if m15[k]['c'] < ref:
                mss_i = k
                break

        if mss_i is None:
            return None

    # Surveillance simultanée des FVG et OB après le MSS.
    zones = []

    for k in range(mss_i, at_index+1):
        z = fvg_at(m15, k, bias)

        if z:
            zones.append(z)

    # OB : dernière bougie opposée avant l'impulsion du MSS.
    ob = None

    for k in range(mss_i-1, max(-1,mss_i-12), -1):
        candle = m15[k]

        if (
            bias == 'BUY' and candle['c'] < candle['o']
        ) or (
            bias == 'SELL' and candle['c'] > candle['o']
        ):
            ob = {
                'kind': 'OB',
                'lo': candle['l'],
                'hi': candle['h'],
                'formed': candle['t'],
                'impulse': mss_i
            }
            break

    if ob:
        zones.append(ob)

    zones.sort(
        key=lambda z: z['formed'],
        reverse=True
    )

    for z in zones:
        if z['formed'] >= m15[at_index]['t']:
            continue

        test = None

        for k in range(z['impulse']+1, at_index):
            c = m15[k]

            touched = (
                c['l'] <= z['hi']
                and c['h'] >= z['lo']
            )

            if touched:
                test = k
                break

        if test is None or test >= at_index:
            continue

        confirm = m15[at_index]

        if (
            bias == 'BUY'
            and confirm['c'] > z['hi']
            and confirm['c'] > confirm['o']
        ):
            a = atr(m15, test) or (
                m15[test]['h']-m15[test]['l']
            )

            entry = confirm['c']
            sl_price = m15[test]['l'] - 0.1*a

            confirmed_highs = [
                p for ix,p in pivots(h4)[0]
                if p > entry
                and ix+2 < len(h4)
                and h4[ix+2]['t']+14400 <= now
            ]

            tp = min(confirmed_highs) if confirmed_highs else None

            if tp and entry > sl_price:
                return {
                    'side': bias,
                    'entry': entry,
                    'sl': sl_price,
                    'tp': tp,
                    'zone': z['kind'],
                    'zone_lo': z['lo'],
                    'zone_hi': z['hi'],
                    'test_time': m15[test]['t'],
                    'confirm_time': confirm['t'],
                    'mss_time': m15[mss_i]['t'],
                    'crt_time': crt['time']
                }

        if (
            bias == 'SELL'
            and confirm['c'] < z['lo']
            and confirm['c'] < confirm['o']
        ):
            a = atr(m15, test) or (
                m15[test]['h']-m15[test]['l']
            )

            entry = confirm['c']
            sl_price = m15[test]['h'] + 0.1*a

            confirmed_lows = [
                p for ix,p in pivots(h4)[1]
                if p < entry
                and ix+2 < len(h4)
                and h4[ix+2]['t']+14400 <= now
            ]

            tp = max(confirmed_lows) if confirmed_lows else None

            if tp and sl_price > entry:
                return {
                    'side': bias,
                    'entry': entry,
                    'sl': sl_price,
                    'tp': tp,
                    'zone': z['kind'],
                    'zone_lo': z['lo'],
                    'zone_hi': z['hi'],
                    'test_time': m15[test]['t'],
                    'confirm_time': confirm['t'],
                    'mss_time': m15[mss_i]['t'],
                    'crt_time': crt['time']
                }

    return None


def scan_closed(h4, m15):
    if len(m15) < 100 or len(h4) < 30:
        return None

    return detect_setup(h4, m15, len(m15)-1)


async def send_notify(title, message):
    if not NTFY_URL:
        return False

    try:
        timeout = aiohttp.ClientTimeout(total=12)

        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post(
                NTFY_URL,
                data=message.encode(),
                headers={
                    'Title': title,
                    'Priority': 'high'
                }
            ) as r:
                return 200 <= r.status < 300

    except Exception as e:
        print('Notify error:', repr(e))
        return False


async def scanner_loop(app):
    await asyncio.sleep(4)

    while True:
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=40)
            ) as http:

                ws = await connect_ws(http)

                try:
                    for sym, name in SYMBOLS:
                        h4 = await fetch_history(
                            ws, sym, 14400,
                            int(time.time())-90*86400,
                            int(time.time())
                        )

                        m15 = await fetch_history(
                            ws, sym, 900,
                            int(time.time())-45*86400,
                            int(time.time())
                        )

                        sig = scan_closed(h4, m15)

                        if sig:
                            key = (
                                sym,
                                sig['side'],
                                sig['confirm_time'],
                                sig['zone']
                            )

                            if key not in _SENT:
                                _SENT.add(key)

                                msg = (
                                    f"{name} {sig['side']} | "
                                    f"zone {sig['zone']} | "
                                    f"entrée {sig['entry']} | "
                                    f"SL {sig['sl']} | "
                                    f"TP liquidité {sig['tp']} | "
                                    f"RR {abs((sig['tp']-sig['entry'])/(sig['entry']-sig['sl'])):.2f}"
                                )

                                SIGNALS.appendleft(msg)
                                STATE['last_signal'] = msg

                                await send_notify(
                                    'ADONIS — signal',
                                    msg
                                )

                        await asyncio.sleep(.15)

                    STATE['scanner'] = 'actif'
                    STATE['last_scan'] = time.time()
                    STATE['last_error'] = None

                finally:
                    await ws.close()

        except asyncio.CancelledError:
            raise

        except Exception as e:
            STATE['scanner'] = 'erreur'
            STATE['last_error'] = str(e)[:240]
            print('Scanner:', repr(e))

        await asyncio.sleep(SCAN_SECONDS)


def simulate_trades(m15, start_index, signal):
    side = signal['side']
    entry = signal['entry']
    sl = signal['sl']
    tp = signal['tp']

    risk = abs(entry-sl)

    if risk <= 0:
        return None

    trail_zones = []
    used_tests = set()

    for k in range(start_index+1, len(m15)):
        c = m15[k]

        # SL prioritaire si SL et TP sont touchés dans la même bougie.
        if side == 'BUY':
            if c['l'] <= sl:
                return {
                    'r': (sl-entry)/risk,
                    'outcome': 'SL/TRAIL',
                    'exit_time': c['t']
                }

            if c['h'] >= tp:
                return {
                    'r': (tp-entry)/risk,
                    'outcome': 'TP',
                    'exit_time': c['t']
                }

        else:
            if c['h'] >= sl:
                return {
                    'r': (entry-sl)/risk,
                    'outcome': 'SL/TRAIL',
                    'exit_time': c['t']
                }

            if c['l'] <= tp:
                return {
                    'r': (entry-tp)/risk,
                    'outcome': 'TP',
                    'exit_time': c['t']
                }

        z = fvg_at(m15, k, side)

        if z:
            trail_zones.append(z)

        test = m15[k-1]

        for zi, z in enumerate(trail_zones):
            if zi in used_tests or z['formed'] >= test['t']:
                continue

            touched = (
                test['l'] <= z['hi']
                and test['h'] >= z['lo']
            )

            confirmed = (
                c['c'] > z['hi'] and c['c'] > c['o']
                if side == 'BUY'
                else c['c'] < z['lo'] and c['c'] < c['o']
            )

            if touched and confirmed:
                a = atr(m15, k-1) or (
                    test['h']-test['l']
                )

                candidate = (
                    test['l']-0.1*a
                    if side == 'BUY'
                    else test['h']+0.1*a
                )

                if side == 'BUY' and sl < candidate < tp:
                    sl = candidate

                if side == 'SELL' and tp < candidate < sl:
                    sl = candidate

                used_tests.add(zi)

    return {
        'r': 0.0,
        'outcome': 'OPEN',
        'exit_time': m15[-1]['t'],
        'final_sl': sl
    }


def stats(trades):
    vals = [x['r'] for x in trades]

    wins = sum(x for x in vals if x > 0)
    losses = -sum(x for x in vals if x < 0)

    eq = peak = dd = 0.0

    for x in vals:
        eq += x
        peak = max(peak, eq)
        dd = max(dd, peak-eq)

    return {
        'trades': len(vals),
        'wins': sum(x > 0 for x in vals),
        'losses': sum(x < 0 for x in vals),
        'open': sum(x == 0 for x in vals),
        'r_total': round(sum(vals), 3),
        'expectancy_r': round(
            sum(vals)/len(vals), 4
        ) if vals else 0,
        'pf': round(wins/losses, 3) if losses else (
            'inf' if wins else 0
        ),
        'max_dd_r': round(dd, 3)
    }


async def run_backtest():
    BT.update(
        running=True,
        done=False,
        progress='Connexion à Deriv…',
        error=None,
        results=None
    )

    start = int(time.time())-BT_DAYS*86400
    end = int(time.time())

    markets = []
    all_trades = []

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=90)
        ) as http:

            ws = await connect_ws(http)

            try:
                for n, (sym, name) in enumerate(SYMBOLS, 1):
                    BT['progress'] = (
                        f'{n}/12 — téléchargement {name}'
                    )

                    try:
                        h4 = await fetch_history(
                            ws, sym, 14400,
                            start-45*86400, end
                        )

                        m15 = await fetch_history(
                            ws, sym, 900,
                            start, end
                        )

                        trades = []
                        last_exit = 0

                        for i in range(100, len(m15)-1):
                            t = m15[i]['t']

                            if t < start or t <= last_exit:
                                continue

                            local_start = max(0, i-700)
                            local_m15 = m15[local_start:i+1]

                            sig = detect_setup(
                                h4,
                                local_m15,
                                len(local_m15)-1
                            )

                            if not sig:
                                continue

                            sim = simulate_trades(m15, i, sig)

                            if sim:
                                trade = {
                                    **sim,
                                    'symbol': sym,
                                    'name': name,
                                    'side': sig['side'],
                                    'zone': sig['zone'],
                                    'entry': sig['entry'],
                                    'sl': sig['sl'],
                                    'tp': sig['tp'],
                                    'signal_time': m15[i]['t']
                                }

                                trades.append(trade)
                                all_trades.append(trade)
                                last_exit = sim['exit_time']

                        st = stats(trades)

                        markets.append({
                            'symbol': sym,
                            'name': name,
                            **st,
                            'error': None
                        })

                    except Exception as e:
                        markets.append({
                            'symbol': sym,
                            'name': name,
                            'error': str(e)[:220],
                            **stats([])
                        })

                    await asyncio.sleep(.2)

            finally:
                await ws.close()

        portfolio = stats(
            sorted(
                all_trades,
                key=lambda x: (x['exit_time'], x['signal_time'])
            )
        )

        BT['results'] = {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'period_days': BT_DAYS,
            'window_start': start,
            'window_end': end,
            'strategy': (
                'ADONIS RESET — H4 structure + 2-candle CRT + '
                'M15 MSS close + FVG/OB retest and separate confirmation'
            ),
            'markets': markets,
            'portfolio': portfolio,
            'trades': all_trades,
            'notes': [
                'Aucun ordre réel.',
                'Les coûts (spread, slippage, commission) ne sont pas inclus.',
                'Le TP utilise la prochaine liquidité H4 identifiée.',
                'La logique de confirmation et le choix OB/FVG '
                'sont une première implémentation fidèle aux règles '
                'disponibles; à valider sur exemples graphiques.'
            ]
        }

        BT.update(
            done=True,
            progress='Terminé'
        )

    except Exception as e:
        BT['error'] = f'{type(e).__name__}: {e}'[:300]

    finally:
        BT['running'] = False


async def start_bt(request):
    if not BT['running']:
        request.app['bt_task'] = asyncio.create_task(
            run_backtest()
        )

    return web.json_response({'running': True})


async def bt_status(request):
    return web.json_response(BT)


async def bt_download(request):
    if not BT['done'] or not BT['results']:
        return web.json_response(
            {'error': 'backtest_not_ready'},
            status=409
        )

    return web.Response(
        text=json.dumps(
            BT['results'],
            ensure_ascii=False,
            indent=2
        ),
        content_type='application/json',
        headers={
            'Content-Disposition':
                'attachment; filename="ADONIS_BACKTEST.json"',
            'Cache-Control': 'no-store'
        }
    )


async def api_status(request):
    return web.json_response({
        'notify': (
            'Notify configuré'
            if NTFY_TOPIC
            else 'NTFY_TOPIC absent — notifications désactivées'
        ),
        'notify_ok': bool(NTFY_TOPIC),
        'scanner': STATE['scanner'],
        'last_scan': STATE['last_scan'],
        'error': STATE['last_error'],
        'last_signal': STATE['last_signal']
    })


async def index(request):
    return web.Response(
        text=HTML,
        content_type='text/html',
        headers={'Cache-Control': 'no-store'}
    )


async def startup(app):
    app['scanner_task'] = asyncio.create_task(
        scanner_loop(app)
    )


async def cleanup(app):
    for key in ('scanner_task', 'bt_task'):
        task = app.get(key)

        if task and not task.done():
            task.cancel()


app = web.Application(client_max_size=2**20)

app.router.add_get('/', index)
app.router.add_get('/api/status', api_status)
app.router.add_post('/api/backtest/start', start_bt)
app.router.add_get('/api/backtest', bt_status)
app.router.add_get('/api/backtest/download', bt_download)

app.on_startup.append(startup)
app.on_cleanup.append(cleanup)


if __name__ == '__main__':
    print(
        'ADONIS reset | symbols:',
        len(SYMBOLS),
        '| Notify:',
        bool(NTFY_TOPIC)
    )

    web.run_app(
        app,
        host='0.0.0.0',
        port=PORT
    )
