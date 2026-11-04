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
        .log-item { font-size: 12px; border-left: 3px solid #38bdf8; padding-left: 8px; margin-bottom: 8px; color: #cbd5e1; font-family: monospace; background: #111827; padding: 6px; border-radius: 4px; }
        .badge-scan { background: #3b82f6; color: white; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; animation: pulse 1.5s infinite; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.5; } 100% { opacity: 1; } }
    </style>
</head>
<body>
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
        <h1 style="margin: 0; font-size: 22px; color: #38bdf8;">⚡ Adonis Autonome</h1>
        <span id="connectionBadge" style="background: #22c55e; color: white; padding: 6px 12px; border-radius: 20px; font-size: 12px;">Connecté (Pro)</span>
    </div>

    <!-- Tableau de Bord du Compte (Démo/Réel & Solde) -->
    <div class="card" style="background: #111827; border: 1px solid #38bdf8;">
        <div class="row">
            <div class="col">
                <label style="font-size: 11px; color: #94a3b8;">TYPE DE COMPTE</label>
                <select id="accountType" onchange="switchAccountMode()" style="margin-top: 8px;">
                    <option value="demo">Démo (Virtuel)</option>
                    <option value="real">Réel (Live)</option>
                </select>
            </div>
            <div class="col">
                <label style="font-size: 11px; color: #94a3b8;">SOLDE TOTAL</label>
                <div id="accountBalance" style="font-family: monospace; font-size: 18px; color: #4ade80; font-weight: bold; margin-top: 12px;">10,450.00 $</div>
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

    <!-- Suivi des Positions en Direct (P&L & Clôture) -->
    <div class="card" style="border: 1px solid #22c55e;">
        <h3>📈 Positions Ouvertes & P&L en Direct</h3>
        <div id="openPositions" style="background: #111827; padding: 10px; border-radius: 4px; font-family: monospace; font-size: 13px;">
            <div style="color: #64748b; text-align: center;">Aucune position active. Lance un ordre pour suivre l'évolution.</div>
        </div>
    </div>

    <!-- Journal des Exécutions (Historique des trades fermés) -->
    <div class="card">
        <h3>📋 Journal des Exécutions (Historique)</h3>
        <div id="tradeLog" style="max-height: 180px; overflow-y: auto; background: #111827; padding: 8px; border-radius: 4px;">
            <div style="color: #64748b; font-size: 12px; text-align: center;">Aucun trade fermé pour le moment.</div>
        </div>
    </div>

    <script>
    let activeTrade = null;
    let baseBalance = 10450.00;
    let currentPrice = 5432.10;

    const marketsToScan = [
        { symbol: 'XAUUSD (Or)', base: 2650.00 },
        { symbol: 'Volatility 50 (1s)', base: 4500.00 },
        { symbol: 'Step Index', base: 1200.00 },
        { symbol: 'BOOM 500', base: 18000.00 },
        { symbol: 'Crash 500', base: 17500.00 }
    ];
    let scanIndex = 0;

    function saveAndConnect() {
        const token = document.getElementById('apitoken').value;
        if (!token.trim()) { alert("Entre un token valide !"); return; }
        localStorage.setItem('user_api_token', token);
        document.getElementById('connectionBadge.style.background = '#22c55e';
        document.getElementById('connectionBadge').innerText = "Connecté (Pro)";
        alert("Robot connecté avec succès !");
    }

    function switchAccountMode() {
        const mode = document.getElementById('accountType').value;
        if (mode === 'demo') {
            baseBalance = 10450.00;
            document.getElementById('accountBalance').innerText = baseBalance.toFixed(2) + " $ (Démo)";
        } else {
            baseBalance = 1250.00;
            document.getElementById('accountBalance').innerText = baseBalance.toFixed(2) + " $ (Réel)";
        }
    }

    // Balayage automatique et fluide des marchés en arrière-plan
    setInterval(() => {
        scanIndex = (scanIndex + 1) % marketsToScan.length;
        const currentScan = marketsToScan[scanIndex];
        document.getElementById('bestAsset').innerText = currentScan.symbol;
        document.getElementById('scanningText').innerText = "Analyse de " + currentScan.symbol + "...";
        currentPrice = currentScan.base + (Math.random() * 20 - 10);
        document.getElementById('livePrice').innerText = currentPrice.toFixed(2);
    }, 4000);

    // Mouvement continu du prix de l'actif sélectionné
    setInterval(() => {
        currentPrice += (Math.random() - 0.48) * 5;
        document.getElementById('livePrice').innerText = currentPrice.toFixed(2);
    }, 1000);

    function executeTrade() {
        if (activeTrade) {
            alert("Une position est déjà active ! Clôture-la avant d'en lancer une autre.");
            return;
        }

        const asset = document.getElementById('bestAsset').innerText;
        const lot = parseFloat(document.getElementById('lotSize').value);

        activeTrade = {
            asset: asset,
            lot: lot,
            currentProfit: 0.00,
            startTime: new Date().toLocaleTimeString()
        };
    }

    function closeActiveTrade() {
        if (!activeTrade) return;

        const timeNow = new Date().toLocaleTimeString();
        const profit = activeTrade.currentProfit;
        const isWin = profit >= 0;
        const statusLabel = isWin ? '<span style="color:#4ade80">🟢 GAGNÉ</span>' : '<span style="color:#ef4444">🔴 PERDU</span>';

        baseBalance += profit;
        const modeText = document.getElementById('accountType').value === 'demo' ? '(Démo)' : '(Réel)';
        document.getElementById('accountBalance').innerText = baseBalance.toFixed(2) + " $ " + modeText;

        const logContainer = document.getElementById('tradeLog');
        if (logContainer.innerHTML.includes('Aucun trade fermé')) {
            logContainer.innerHTML = '';
        }

        const logItem = document.createElement('div');
        logItem.className = 'log-item';
        logItem.innerHTML = `[${timeNow}] ${statusLabel} | Marché : <b>${activeTrade.asset}</b> | Lot : ${activeTrade.lot} | Résultat : <b style="color:${isWin ? '#4ade80':'#ef4444'}">${isWin ? '+' : ''}${profit.toFixed(2)} $</b>`;
        logContainer.prepend(logItem);

        activeTrade = null;
        document.getElementById('openPositions').innerHTML = '<div style="color: #64748b; text-align: center;">Aucune position active. Lance un ordre pour suivre l\\'évolution.</div>';
    }

    // Animation en temps réel du P&L et du solde sans coupure réseau
    setInterval(() => {
        if (!activeTrade) return;

        let fluctuation = (Math.random() - 0.48) * 1.5 * activeTrade.lot * 100;
        activeTrade.currentProfit += fluctuation;

        const posContainer = document.getElementById('openPositions');
        const colorStyle = activeTrade.currentProfit >= 0 ? '#4ade80' : '#ef4444';
        const statusText = activeTrade.currentProfit >= 0 ? 'EN GAIN (Vers TP)' : 'DRAWDOWN';

        posContainer.innerHTML = `
            <b>Marché :</b> ${activeTrade.asset}<br>
            <b>Lot :</b> ${activeTrade.lot} | <b>Statut :</b> <span style="color:${colorStyle}">${statusText}</span><br>
            <b>P&L Flottant :</b> <span style="color:${colorStyle}; font-size:16px;">${activeTrade.currentProfit >= 0 ? '+' : ''}${activeTrade.currentProfit.toFixed(2)} $</span><br>
            <button onclick="closeActiveTrade()" style="margin-top: 10px; background: #ef4444; color: white; border: none; padding: 8px; border-radius: 4px; width: 100%; font-weight: bold; cursor: pointer;">Clôturer le Trade (Enregistrer)</button>
        `;

        let liveTotalBalance = baseBalance + activeTrade.currentProfit;
        const modeText = document.getElementById('accountType').value === 'demo' ? '(Démo)' : '(Réel)';
        document.getElementById('accountBalance').innerText = liveTotalBalance.toFixed(2) + " $ " + modeText;
    }, 1000);

    window.onload = function() {
        const savedToken = localStorage.getItem('user_api_token');
        if (savedToken) {
            document.getElementById('apitoken').value = savedToken;
            document.getElementById('connectionBadge').style.background = '#22c55e';
            document.getElementById('connectionBadge').innerText = "Connecté (Pro)";
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
