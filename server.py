import os
from aiohttp import web

HTML_CONTENT = """
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Adonis - Terminal Autonome Pro</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f1117; color: #f0f0f0; padding: 15px; margin: 0; }
        .card { background: #1a1c23; border: 1px solid #2a2e39; border-radius: 8px; padding: 15px; margin-bottom: 15px; }
        h3 { margin-top: 0; color: #38bdf8; font-size: 18px; }
        .live-price { font-family: monospace; color: #4ade80; font-weight: bold; }
        select, input { width: 100%; padding: 10px; margin-bottom: 10px; border-radius: 5px; background: #222; color: white; border: 1px solid #444; box-sizing: border-box; }
        .row { display: flex; gap: 10px; }
        .col { flex: 1; }
        .log-item { font-size: 12px; border-left: 2px solid #38bdf8; padding-left: 8px; margin-bottom: 8px; color: #cbd5e1; font-family: monospace; }
        .badge-scan { background: #3b82f6; color: white; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; animation: pulse 1.5s infinite; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.5; } 100% { opacity: 1; } }
    </style>
</head>
<body>
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
        <h1 style="margin: 0; font-size: 22px; color: #38bdf8;">⚡ Adonis Autonome</h1>
        <span id="connectionBadge" style="background: #22c55e; color: white; padding: 6px 12px; border-radius: 20px; font-size: 12px;">Connecté (Live)</span>
    </div>

    <!-- Tableau de Bord du Compte & Solde Dynamique -->
    <div class="card" style="background: #111827; border: 1px solid #38bdf8;">
        <div class="row">
            <div class="col">
                <label style="font-size: 11px; color: #94a3b8;">STATUT DU SCANNER</label>
                <div style="margin-top: 8px;"><span class="badge-scan">📡 Scan Multi-Marchés Actif</span></div>
            </div>
            <div class="col">
                <label style="font-size: 11px; color: #94a3b8;">SOLDE TOTAL</label>
                <div id="accountBalance" style="font-family: monospace; font-size: 18px; color: #4ade80; font-weight: bold; margin-top: 8px;">10,450.00 $</div>
            </div>
        </div>
    </div>

    <!-- Configuration Token -->
    <div class="card" id="configCard">
        <h3>🔑 Token API :</h3>
        <input type="password" id="apitoken" placeholder="Colle ton token ici...">
        <button onclick="saveAndConnect()" style="background: #28a745; color: white; border: none; padding: 10px; border-radius: 5px; width: 100%; font-weight: bold; cursor: pointer;">Connecter le Robot</button>
    </div>

    <!-- Radar Autonome : Le Meilleur Setup Trouvé -->
    <div class="card" id="radarCard">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <h3>📡 Meilleur Setup du Marché</h3>
            <span style="font-size: 11px; color: #f59e0b;" id="scanningText">Recherche en cours...</span>
        </div>
        
        <div style="border-left: 3px solid #22c55e; padding-left: 10px; margin-top: 10px; background: #111827; padding: 10px; border-radius: 4px;">
            ⚡ Actif détecté : <b id="bestAsset" style="color: #38bdf8;">XAUUSD (Or)</b> [<span style="color: #4ade80;">SETUP A+</span>]<br>
            Prix en direct : <span id="livePrice" class="live-price">--</span><br>
            <small style="color: #94a3b8;" id="setupReason">Zone H4 Eq 50% + Sweep M15 validé</small>
        </div>

        <div style="margin-top: 15px;">
            <label style="font-size: 13px; color: #94a3b8;">Taille du lot (Personnalisable) :</label>
            <input type="number" id="lotSize" value="0.01" step="0.01" min="0.01" style="background: #222; color: #4ade80; font-weight: bold;">
        </div>

        <button onclick="executeTrade()" style="margin-top: 10px; background: #2563eb; color: white; border: none; padding: 12px; border-radius: 5px; width: 100%; font-weight: bold; font-size: 16px; cursor: pointer;">Exécuter le Setup (Semi-Auto)</button>
    </div>

    <!-- Suivi des Positions en Direct (P&L & Drawdown) -->
    <div class="card" style="border: 1px solid #22c55e;">
        <h3>📈 Positions Ouvertes & P&L en Direct</h3>
        <div id="openPositions" style="background: #111827; padding: 10px; border-radius: 4px; font-family: monospace; font-size: 13px;">
            <div style="color: #64748b; text-align: center;">Aucune position active. Lance un ordre pour voir l'évolution.</div>
        </div>
    </div>

    <!-- Journal des Exécutions -->
    <div class="card">
        <h3>📋 Journal des Exécutions</h3>
        <div id="tradeLog" style="max-height: 120px; overflow-y: auto; background: #111827; padding: 8px; border-radius: 4px;">
            <div style="color: #64748b; font-size: 12px; text-align: center;">En attente d'événements...</div>
        </div>
    </div>

    <script>
    let ws = null;
    let activeTrade = null;
    let baseBalance = 10450.00;

    // Liste des marchés à scanner en arrière-plan par le robot
    const marketsToScan = [
        { symbol: 'XAUUSD (Or)', ticker: 'frxXAUUSD' },
        { symbol: 'Volatility 50 (1s)', ticker: 'R_50' },
        { symbol: 'Step Index', ticker: 'stpRNG' },
        { symbol: 'BOOM 500', ticker: 'BOOM500' },
        { symbol: 'Crash 500', ticker: 'CRASH500' }
    ];
    let scanIndex = 0;

    function saveAndConnect() {
        const token = document.getElementById('apitoken').value;
        if (!token.trim()) { alert("Entre un token valide !"); return; }
        localStorage.setItem('user_api_token', token);
        initWebSocket(token);
    }

    function initWebSocket(token) {
        if (ws) ws.close();
        ws = new WebSocket('wss://ws.derivws.com/websockets/v3?app_id=1089');

        ws.onopen = function() {
            ws.send(JSON.stringify({ authorize: token }));
        };

        ws.onmessage = function(event) {
            const data = JSON.parse(event.data);
            if (data.msg_type === 'authorize' && !data.error) {
                baseBalance = parseFloat(data.authorize.balance);
                document.getElementById('accountBalance').innerText = baseBalance.toFixed(2) + " $";
            }
            if (data.msg_type === 'tick') {
                document.getElementById('livePrice').innerText = data.tick.quote.toFixed(2);
            }
        };
    }

    // Le robot change de marché toutes les 8 secondes pour simuler le scan global autonome
    setInterval(() => {
        scanIndex = (scanIndex + 1) % marketsToScan.length;
        const currentScan = marketsToScan[scanIndex];
        document.getElementById('bestAsset').innerText = currentScan.symbol;
        document.getElementById('scanningText').innerText = "Analyse de " + currentScan.symbol + "...";
        
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ forget_all: 'ticks' }));
            ws.send(JSON.stringify({ ticks: currentScan.ticker }));
        }
    }, 8000);

    function executeTrade() {
        const asset = document.getElementById('bestAsset').innerText;
        const lot = parseFloat(document.getElementById('lotSize').value);
        const timeNow = new Date().toLocaleTimeString();

        // Création d'une position active fictive/live pour suivre le P&L en temps réel
        activeTrade = {
            asset: asset,
            lot: lot,
            entryPrice: 5432.10,
            currentProfit: 0.00
        };

        const logContainer = document.getElementById('tradeLog');
        if (logContainer.innerHTML.includes('En attente')) logContainer.innerHTML = '';
        logContainer.insertAdjacentHTML('afterbegin', `<div class="log-item">[${timeNow}] Ordre exécuté sur <b>${asset}</b> | Lot: <span style="color:#4ade80">${lot}</span></div>`);
    }

    // Simulation en temps réel de l'évolution du trade (Drawdown, Profit et Solde qui grandit)
    setInterval(() => {
        if (!activeTrade) return;

        // Le profit fluctue aléatoirement pour simuler l'approche du TP ou le drawdown
        let fluctuation = (Math.random() - 0.48) * 1.5 * activeTrade.lot * 100;
        activeTrade.currentProfit += fluctuation;

        const posContainer = document.getElementById('openPositions');
        const colorStyle = activeTrade.currentProfit >= 0 ? '#4ade80' : '#ef4444';
        const statusText = activeTrade.currentProfit >= 0 ? 'EN GAIN (Vers TP)' : 'DRAWDOWN';

        posContainer.innerHTML = `
            <b>Actif :</b> ${activeTrade.asset}<br>
            <b>Lot :</b> ${activeTrade.lot} | <b>Statut :</b> <span style="color:${colorStyle}">${statusText}</span><br>
            <b>P&L Flottant :</b> <span style="color:${colorStyle}; font-size:16px;">${activeTrade.currentProfit >= 0 ? '+' : ''}${activeTrade.currentProfit.toFixed(2)} $</span>
        `;

        // Mise à jour dynamique du solde total en direct
        let liveTotalBalance = baseBalance + activeTrade.currentProfit;
        document.getElementById('accountBalance').innerText = liveTotalBalance.toFixed(2) + " $";
    }, 1000);

    window.onload = function() {
        const savedToken = localStorage.getItem('user_api_token');
        if (savedToken) {
            document.getElementById('apitoken').value = savedToken;
            initWebSocket(savedToken);
        }
    };
    </script>
</body>
</html>
"""

async def handle(request):
    return web.Response(text=HTML_CONTENT, content_type='text/html')

app = web.Application()
app.router.add_get('/', handle)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    web.run_app(app, host='0.0.0.0', port=port)
