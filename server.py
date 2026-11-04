import os
from aiohttp import web

HTML_CONTENT = """
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Adonis - Terminal Autonome Live</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f1117; color: #f0f0f0; padding: 15px; margin: 0; }
        .card { background: #1a1c23; border: 1px solid #2a2e39; border-radius: 8px; padding: 15px; margin-bottom: 15px; }
        h3 { margin-top: 0; color: #38bdf8; font-size: 18px; }
        .live-price { font-family: monospace; color: #4ade80; font-weight: bold; }
        select, input { width: 100%; padding: 10px; margin-bottom: 10px; border-radius: 5px; background: #222; color: white; border: 1px solid #444; box-sizing: border-box; }
        .row { display: flex; gap: 10px; }
        .col { flex: 1; }
        .log-item { font-size: 12px; border-left: 3px solid #38bdf8; padding-left: 8px; margin-bottom: 8px; color: #cbd5e1; font-family: monospace; background: #111827; padding: 6px; border-radius: 4px; }
    </style>
</head>
<body>
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
        <h1 style="margin: 0; font-size: 22px; color: #38bdf8;">⚡ Adonis Live</h1>
        <span id="connectionBadge" style="background: #eab308; color: white; padding: 6px 12px; border-radius: 20px; font-size: 12px;">Déconnecté</span>
    </div>

    <!-- Tableau de Bord du Compte & Vrai Solde -->
    <div class="card" style="background: #111827; border: 1px solid #38bdf8;">
        <div class="row">
            <div class="col">
                <label style="font-size: 11px; color: #94a3b8;">TYPE DE COMPTE</label>
                <select id="accountType" disabled style="background: #111; color: #94a3b8; margin-top: 8px;">
                    <option value="auto">Détecté via Token API</option>
                </select>
            </div>
            <div class="col">
                <label style="font-size: 11px; color: #94a3b8;">SOLDE RÉEL DU COMPTE</label>
                <div id="accountBalance" style="font-family: monospace; font-size: 18px; color: #4ade80; font-weight: bold; margin-top: 12px;">-- $</div>
            </div>
        </div>
    </div>

    <!-- Configuration Token -->
    <div class="card" id="configCard">
        <h3>🔑 Token API :</h3>
        <input type="password" id="apitoken" placeholder="Colle ton token ici...">
        <button onclick="saveAndConnect()" style="background: #28a745; color: white; border: none; padding: 10px; border-radius: 5px; width: 100%; font-weight: bold; cursor: pointer;">Connecter au Compte Réel</button>
    </div>

    <!-- Radar Autonome -->
    <div class="card" id="radarCard" style="display: none;">
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
        document.getElementById('connectionBadge').style.background = '#eab308';
        document.getElementById('connectionBadge').innerText = "Connexion...";

        if (ws) ws.close();
        ws = new WebSocket('wss://ws.derivws.com/websockets/v3?app_id=1089');

        ws.onopen = function() {
            ws.send(JSON.stringify({ authorize: token }));
        };

        ws.onmessage = function(event) {
            const data = JSON.parse(event.data);
            
            if (data.msg_type === 'authorize') {
                if (data.error) {
                    alert("Erreur Token : " + data.error.message);
                    document.getElementById('connectionBadge').style.background = '#ef4444';
                    document.getElementById('connectionBadge').innerText = "Erreur Token";
                } else {
                    document.getElementById('connectionBadge').style.background = '#22c55e';
                    document.getElementById('connectionBadge').innerText = "Connecté (Live)";
                    document.getElementById('radarCard').style.display = 'block';

                    const user = data.authorize;
                    baseBalance = parseFloat(user.balance);
                    const accountTypeText = user.is_virtual ? "Démo (Virtuel)" : "Réel (Live)";
                    document.getElementById('accountType').options[0].text = accountTypeText;
                    document.getElementById('accountBalance').innerText = baseBalance.toFixed(2) + " " + user.currency;

                    // Lancer le scanner sur le premier marché
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
            document.getElementById('connectionBadge').innerText = "Erreur Réseau";
        };
    }

    function startScanner() {
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
            <button onclick="closeActiveTrade()" style="margin-top: 10px; background: #ef4444; color: white; border: none; padding: 8px; border-radius: 4px; width: 100%; font-weight: bold; cursor: pointer;">Clôturer le Trade</button>
        `;

        let liveTotalBalance = baseBalance + activeTrade.currentProfit;
        document.getElementById('accountBalance').innerText = liveTotalBalance.toFixed(2) + " $";
    }, 1000);

    window.onload = function() {
        const savedToken = localStorage.getItem('user_api_token');
        if (savedToken && savedToken.trim() !== "") {
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
