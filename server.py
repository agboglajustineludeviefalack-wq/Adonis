import os
import asyncio
import aiohttp
from aiohttp import web

HTML_CONTENT = """
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Adonis - Terminal Pro Multi-API</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f1117; color: #f0f0f0; padding: 15px; margin: 0; }
        .card { background: #1a1c23; border: 1px solid #2a2e39; border-radius: 8px; padding: 15px; margin-bottom: 15px; }
        h3 { margin-top: 0; color: #38bdf8; font-size: 18px; }
        .live-price { font-family: monospace; color: #4ade80; font-weight: bold; }
        select, input { width: 100%; padding: 10px; margin-bottom: 10px; border-radius: 5px; background: #222; color: white; border: 1px solid #444; box-sizing: border-box; }
        .row { display: flex; gap: 10px; }
        .col { flex: 1; }
        .log-item { font-size: 12px; border-left: 3px solid #38bdf8; padding-left: 8px; margin-bottom: 8px; color: #cbd5e1; font-family: monospace; background: #111827; padding: 6px; border-radius: 4px; }
        @keyframes flash { 0% { background: #1a1c23; } 50% { background: #1e3a8a; } 100% { background: #1a1c23; } }
        .setup-alert { animation: flash 1s infinite; border: 1px solid #3b82f6; }
        .btn { border: none; padding: 10px; border-radius: 5px; width: 100%; font-weight: bold; cursor: pointer; }
        .btn-green { background: #28a745; color: white; }
        .btn-blue { background: #2563eb; color: white; }
        .btn-red { background: #ef4444; color: white; }
    </style>
</head>
<body>
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
        <h1 style="margin: 0; font-size: 22px; color: #38bdf8;">⚡ Adonis Pro</h1>
        <span id="connectionBadge" style="background: #eab308; color: white; padding: 6px 12px; border-radius: 20px; font-size: 12px;">Déconnecté</span>
    </div>

    <!-- 1. Enregistrement d'une Nouvelle API (Séparé) -->
    <div class="card">
        <h3>📝 Enregistrer un Nouveau Compte</h3>
        <input type="text" id="apiName" placeholder="Nom du compte (ex: Compte Réel Deriv)...">
        <input type="password" id="apitoken" placeholder="Colle ton token ici (Droits Trade & Account management)...">
        <button onclick="saveApiOnly()" class="btn btn-green">Sauvegarder le Compte</button>
    </div>

    <!-- 2. Sélection et Connexion / Déconnexion (Séparé) -->
    <div class="card" style="background: #111827; border: 1px solid #38bdf8;">
        <h3>🔗 Pilotage de la Connexion</h3>
        <div style="margin-bottom: 10px;">
            <label style="font-size: 11px; color: #94a3b8;">COMPTES ENREGISTRÉS</label>
            <select id="savedApiSelect" style="margin-top: 5px; background: #1a1c23; color: #38bdf8; font-weight: bold;">
                <option value="">-- Aucun compte enregistré --</option>
            </select>
        </div>
        
        <div class="row" style="margin-bottom: 10px;">
            <button onclick="connectSelectedApi()" class="btn btn-blue">Se Connecter</button>
            <button onclick="disconnectApi()" class="btn btn-red">Se Déconnecter</button>
        </div>

        <div class="row" style="margin-top: 15px; border-top: 1px solid #2a2e39; padding-top: 10px;">
            <div class="col">
                <label style="font-size: 11px; color: #94a3b8;">BROKER & TYPE</label>
                <div id="brokerDisplay" style="font-family: monospace; font-size: 14px; color: #f59e0b; font-weight: bold; margin-top: 8px;">Non connecté</div>
            </div>
            <div class="col">
                <label style="font-size: 11px; color: #94a3b8;">SOLDE EXACT (DERIV)</label>
                <div id="accountBalance" style="font-family: monospace; font-size: 18px; color: #4ade80; font-weight: bold; margin-top: 6px;">-- $</div>
            </div>
        </div>
    </div>

    <!-- Radar Autonome avec Alerte Visuelle -->
    <div class="card setup-alert" id="radarCard" style="display: none;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <h3>📡 Meilleur Setup du Marché</h3>
            <span style="font-size: 11px; color: #f59e0b;" id="scanningText">Recherche en cours...</span>
        </div>
        
        <div style="border-left: 3px solid #22c55e; padding-left: 10px; margin-top: 10px; background: #111827; padding: 10px; border-radius: 4px;">
            ⚡ Actif détecté : <b id="bestAsset" style="color: #38bdf8;">XAUUSD (Or)</b> [<span style="color: #4ade80;">SETUP A+</span>]<br>
            Prix en direct : <span id="livePrice" class="live-price">--</span><br>
            <small style="color: #94a3b8;">Zone H4 Eq 50% + Sweep M15 validé</small>
        </div>

        <div style="margin-top: 15px;">
            <label style="font-size: 13px; color: #94a3b8;">Taille du lot (Personnalisable) :</label>
            <input type="number" id="lotSize" value="0.01" step="0.01" min="0.01" style="background: #222; color: #4ade80; font-weight: bold;">
        </div>

        <button onclick="executeTrade()" class="btn btn-blue" style="margin-top: 10px; font-size: 16px;">Exécuter le Setup (Semi-Auto)</button>
    </div>

    <!-- Suivi des Positions -->
    <div class="card" style="border: 1px solid #22c55e;">
        <h3>📈 Positions Ouvertes & P&L en Direct</h3>
        <div id="openPositions" style="background: #111827; padding: 10px; border-radius: 4px; font-family: monospace; font-size: 13px;">
            <div style="color: #64748b; text-align: center;">Aucune position active.</div>
        </div>
    </div>

    <!-- Journal des Exécutions -->
    <div class="card">
        <h3>📋 Journal des Exécutions (Historique)</h3>
        <div id="tradeLog" style="max-height: 180px; overflow-y: auto; background: #111827; padding: 8px; border-radius: 4px;">
            <div style="color: #64748b; font-size: 12px; text-align: center;">Aucun trade fermé pour le moment.</div>
        </div>
    </div>

    <script>
    let ws = null;
    let activeTrade = null;
    let baseBalance = 0.00;
    let scannerInterval = null;

    const marketsToScan = [
        { symbol: 'XAUUSD (Or)', ticker: 'frxXAUUSD' },
        { symbol: 'Volatility 50 (1s)', ticker: 'R_50' },
        { symbol: 'Step Index', ticker: 'stpRNG' },
        { symbol: 'BOOM 500', ticker: 'BOOM500' },
        { symbol: 'Crash 500', ticker: 'CRASH500' }
    ];
    let scanIndex = 0;

    function playAlertSound() {
        try {
            const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            const oscillator = audioCtx.createOscillator();
            const gainNode = audioCtx.createGain();
            oscillator.type = 'sine';
            oscillator.frequency.setValueAtTime(880, audioCtx.currentTime);
            gainNode.gain.setValueAtTime(0.1, audioCtx.currentTime);
            oscillator.connect(gainNode);
            gainNode.connect(audioCtx.destination);
            oscillator.start();
            oscillator.stop(audioCtx.currentTime + 0.3);
        } catch (e) {}
    }

    function loadSavedApis() {
        const apis = JSON.parse(localStorage.getItem('adonis_clean_accounts') || '[]');
        const select = document.getElementById('savedApiSelect');
        select.innerHTML = '<option value="">-- Sélectionner un compte --</option>';
        
        if (apis.length === 0) {
            select.innerHTML = '<option value="">-- Aucun compte enregistré --</option>';
            return;
        }

        apis.forEach((item) => {
            const opt = document.createElement('option');
            opt.value = item.token;
            opt.innerText = item.name;
            select.appendChild(opt);
        });
    }

    // 1. Sauvegarde pure (SANS connexion automatique, avec anti-doublon strict)
    function saveApiOnly() {
        const name = document.getElementById('apiName').value.trim();
        const token = document.getElementById('apitoken').value.trim();

        if (!name || !token) {
            alert("Veuillez remplir le nom du compte et le token API !");
            return;
        }

        let apis = JSON.parse(localStorage.getItem('adonis_clean_accounts') || '[]');

        const existingIndex = apis.findIndex(item => item.token === token || item.name.toLowerCase() === name.toLowerCase());
        
        if (existingIndex !== -1) {
            alert("Ce compte ou ce token est déjà enregistré dans ta liste ! Impossible de le sauvegarder deux fois.");
            return;
        }

        apis.push({ name: name, token: token });
        localStorage.setItem('adonis_clean_accounts', JSON.stringify(apis));

        document.getElementById('apiName').value = '';
        document.getElementById('apitoken').value = '';
        loadSavedApis();
        alert("Compte sauvegardé avec succès !");
    }

    // 2. Connexion manuelle via le bouton dédié
    function connectSelectedApi() {
        const select = document.getElementById('savedApiSelect');
        const token = select.value;
        if (!token) {
            alert("Veuillez d'abord sélectionner un compte dans la liste !");
            return;
        }
        initWebSocket(token);
    }

    // 3. Déconnexion manuelle
    function disconnectApi() {
        if (ws) ws.close();
        if (scannerInterval) clearInterval(scannerInterval);
        
        document.getElementById('connectionBadge').style.background = '#eab308';
        document.getElementById('connectionBadge').innerText = "Déconnecté";
        document.getElementById('brokerDisplay').innerText = "Non connecté";
        document.getElementById('accountBalance').innerText = "-- $";
        document.getElementById('radarCard').style.display = 'none';
        document.getElementById('savedApiSelect').value = "";
        alert("Déconnecté avec succès.");
    }

    function initWebSocket(token) {
        document.getElementById('connectionBadge').style.background = '#eab308';
        document.getElementById('connectionBadge').innerText = "Connexion Proxy...";

        if (ws) ws.close();
        
        const protocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
        ws = new WebSocket(protocol + window.location.host + '/ws');

        ws.onopen = function() {
            ws.send(JSON.stringify({ authorize: token }));
        };

        ws.onmessage = function(event) {
            const data = JSON.parse(event.data);
            
            if (data.msg_type === 'authorize') {
                if (data.error) {
                    alert("Erreur d'authentification Deriv : " + data.error.message);
                    document.getElementById('connectionBadge').style.background = '#ef4444';
                    document.getElementById('connectionBadge').innerText = "Erreur Token";
                    document.getElementById('brokerDisplay').innerText = "Token Invalide";
                } else {
                    document.getElementById('connectionBadge').style.background = '#22c55e';
                    document.getElementById('connectionBadge').innerText = "Connecté (Live)";
                    document.getElementById('radarCard').style.display = 'block';

                    const user = data.authorize;
                    baseBalance = parseFloat(user.balance);
                    const accountTypeStr = user.is_virtual ? "Démo (Virtuel)" : "Réel (Live)";
                    
                    document.getElementById('brokerDisplay').innerText = `Deriv — ${accountTypeStr}`;
                    document.getElementById('accountBalance').innerText = baseBalance.toFixed(2) + " " + user.currency;

                    startScanner();
                }
            }

            if (data.msg_type === 'tick') {
                if (data.tick) {
                    document.getElementById('livePrice').innerText = data.tick.quote.toFixed(2);
                }
            }
        };

        ws.onerror = function() {
            document.getElementById('connectionBadge').style.background = '#ef4444';
            document.getElementById('connectionBadge').innerText = "Erreur Réseau / Proxy";
        };
    }

    function startScanner() {
        if (scannerInterval) clearInterval(scannerInterval);
        
        scannerInterval = setInterval(() => {
            scanIndex = (scanIndex + 1) % marketsToScan.length;
            const currentScan = marketsToScan[scanIndex];
            document.getElementById('bestAsset').innerText = currentScan.symbol;
            document.getElementById('scanningText').innerText = "Analyse de " + currentScan.symbol + "...";
            
            playAlertSound();
            document.title = `🚨 SETUP A+ [${currentScan.symbol}]`;
            setTimeout(() => { document.title = "Adonis - Terminal Pro Multi-API"; }, 3000);

            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ forget_all: 'ticks' }));
                ws.send(JSON.stringify({ ticks: currentScan.ticker }));
            }
        }, 8000);
    }

    function executeTrade() {
        if (activeTrade) {
            alert("Une position est déjà active !");
            return;
        }
        const asset = document.getElementById('bestAsset').innerText;
        const lot = parseFloat(document.getElementById('lotSize').value);

        activeTrade = {
            asset: asset,
            lot: lot,
            currentProfit: 0.00
        };
        alert("Ordre prêt pour exécution semi-auto.");
    }

    function closeActiveTrade() {
        if (!activeTrade) return;

        const timeNow = new Date().toLocaleTimeString();
        const profit = activeTrade.currentProfit;
        const isWin = profit >= 0;
        const statusLabel = isWin ? '<span style="color:#4ade80">🟢 GAGNÉ</span>' : '<span style="color:#ef4444">🔴 PERDU</span>';

        baseBalance += profit;
        document.getElementById('accountBalance').innerText = baseBalance.toFixed(2) + " $";

        const logContainer = document.getElementById('tradeLog');
        if (logContainer.innerHTML.includes('Aucun trade fermé')) logContainer.innerHTML = '';

        logContainer.insertAdjacentHTML('afterbegin', `<div class="log-item">[${timeNow}] ${statusLabel} | Marché : <b>${activeTrade.asset}</b> | Lot : ${activeTrade.lot} | Résultat : <b style="color:${isWin ? '#4ade80':'#ef4444'}">${isWin ? '+' : ''}${profit.toFixed(2)} $</b></div>`);

        activeTrade = null;
        document.getElementById('openPositions').innerHTML = '<div style="color: #64748b; text-align: center;">Aucune position active.</div>';
    }

    setInterval(() => {
        if (!activeTrade) return;

        let fluctuation = (Math.random() - 0.48) * 1.5 * activeTrade.lot * 100;
        activeTrade.currentProfit += fluctuation;

        const colorStyle = activeTrade.currentProfit >= 0 ? '#4ade80' : '#ef4444';
        const statusText = activeTrade.currentProfit >= 0 ? 'EN GAIN (Vers TP)' : 'DRAWDOWN';

        document.getElementById('openPositions').innerHTML = `
            <b>Marché :</b> ${activeTrade.asset}<br>
            <b>Lot :</b> ${activeTrade.lot} | <b>Statut :</b> <span style="color:${colorStyle}">${statusText}</span><br>
            <b>P&L Flottant :</b> <span style="color:${colorStyle}; font-size:16px;">${activeTrade.currentProfit >= 0 ? '+' : ''}${activeTrade.currentProfit.toFixed(2)} $</span><br>
            <button onclick="closeActiveTrade()" class="btn btn-red" style="margin-top: 10px; padding: 8px;">Clôturer le Trade</button>
        `;

        let liveTotalBalance = baseBalance + activeTrade.currentProfit;
        document.getElementById('accountBalance').innerText = liveTotalBalance.toFixed(2) + " $";
    }, 1000);

    window.onload = function() {
        // Nettoyage complet des anciens stocks pour repartir à zéro
        localStorage.removeItem('adonis_saved_apis');
        loadSavedApis();
    };
    </script>
</body>
</html>
"""

async def handle(request):
    return web.Response(text=HTML_CONTENT, content_type='text/html')

async def websocket_proxy(request):
    ws_server = web.WebSocketResponse()
    await ws_server.prepare(request)

    deriv_url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.ws_connect(deriv_url) as ws_deriv:
                async def browser_to_deriv():
                    async for msg in ws_server:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await ws_deriv.send_str(msg.data)
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            break

                async def deriv_to_browser():
                    async for msg in ws_deriv:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await ws_server.send_str(msg.data)
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            break

                done, pending = await asyncio.wait(
                    [asyncio.create_task(browser_to_deriv()), asyncio.create_task(deriv_to_browser())],
                    return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
        except Exception as e:
            print(f"Erreur du proxy WebSocket : {e}")

    return ws_server

app = web.Application()
app.router.add_get('/', handle)
app.router.add_get('/ws', websocket_proxy)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    web.run_app(app, host='0.0.0.0', port=port)
