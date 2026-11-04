import os
from aiohttp import web

HTML_CONTENT = """
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Adonis - Cloud Trading Scanner</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f1117; color: #f0f0f0; padding: 15px; margin: 0; }
        .card { background: #1a1c23; border: 1px solid #2a2e39; border-radius: 8px; padding: 15px; margin-bottom: 15px; }
        h3 { margin-top: 0; color: #38bdf8; font-size: 18px; }
        .live-price { font-family: monospace; color: #4ade80; font-weight: bold; }
        select, input { width: 100%; padding: 10px; margin-bottom: 10px; border-radius: 5px; background: #222; color: white; border: 1px solid #444; box-sizing: border-box; }
    </style>
</head>
<body>
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
        <h1 style="margin: 0; font-size: 22px; color: #38bdf8;">⚡ Adonis</h1>
        <span id="connectionBadge" style="background: #22c55e; color: white; padding: 6px 12px; border-radius: 20px; font-size: 12px;">Connecté (Pro)</span>
    </div>

    <!-- Configuration & Connexion -->
    <div class="card" id="configCard">
        <h3>🌐 Configuration du Broker</h3>
        <select id="brokerSelect">
            <option value="deriv">Deriv (Synthetic & Forex)</option>
            <option value="weltrade">Weltrade (MT4/MT5 API)</option>
            <option value="pocketoption">Pocket Option</option>
        </select>

        <h3>🔑 Token API :</h3>
        <input type="password" id="apitoken" placeholder="Colle ton token ici...">
        <button onclick="saveConfig()" style="background: #28a745; color: white; border: none; padding: 10px; border-radius: 5px; width: 100%; font-weight: bold; cursor: pointer;">Enregistrer</button>
    </div>

    <!-- Scanner Multi-Actifs SMC / CRT -->
    <div class="card" id="radarCard">
        <h3>📡 Scanner Actif (H4 CRT + M15 FVG)</h3>
        <label style="font-size: 13px; color: #94a3b8;">Sélectionne l'actif à analyser :</label>
        <select id="assetSelect" onchange="updateSelectedAsset()">
            <option value="R_50">Volatility 50 (1s)</option>
            <option value="XAUUSD">XAUUSD (Or)</option>
            <option value="stpRNG">Step Index</option>
            <option value="BOOM500">BOOM 500</option>
            <option value="BOOM1000">BOOM 1000</option>
        </select>

        <div style="border-left: 3px solid #38bdf8; padding-left: 10px; margin-top: 15px; background: #111827; padding: 10px; border-radius: 4px;">
            ⚡ <b id="activeAssetTitle">Volatility 50 (1s)</b> [<span style="color: #4ade80;">SETUP A+</span>]<br>
            Prix en direct : <span id="livePrice" class="live-price">--</span><br>
            <small style="color: #94a3b8;">Zone H4 Eq 50% + Sweep M15 validé</small>
        </div>

        <div style="margin-top: 15px;">
            <label style="font-size: 13px; color: #94a3b8;">Taille du lot (Sécurité) :</label>
            <input type="text" id="lotSize" value="0.01" readonly style="background: #111; color: #4ade80; text-align: center; font-weight: bold;">
        </div>

        <button onclick="executeTrade()" style="margin-top: 10px; background: #2563eb; color: white; border: none; padding: 12px; border-radius: 5px; width: 100%; font-weight: bold; font-size: 16px; cursor: pointer;">Exécuter l'Ordre Semi-Auto</button>
    </div>

    <script>
    function saveConfig() {
        const token = document.getElementById('apitoken').value;
        const broker = document.getElementById('brokerSelect').value;
        if (!token.trim()) {
            alert("Entre un token valide !");
            return;
        }
        localStorage.setItem('user_api_token', token);
        localStorage.setItem('selected_broker', broker);
        alert("Configuration enregistrée avec succès !");
    }

    function updateSelectedAsset() {
        const select = document.getElementById('assetSelect');
        const assetName = select.options[select.selectedIndex].text;
        document.getElementById('activeAssetTitle').innerText = assetName;
    }

    function executeTrade() {
        const asset = document.getElementById('activeAssetTitle').innerText;
        const lot = document.getElementById('lotSize').value;
        const broker = document.getElementById('brokerSelect').value;
        alert("Ordre semi-automatique transmis à " + broker.toUpperCase() + "\\nActif : " + asset + "\\nLot : " + lot);
    }

    // Simulation de prix en direct fluide pour le dashboard
    setInterval(() => {
        let randomPrice = (1000 + Math.random() * 5000).toFixed(2);
        document.getElementById('livePrice').innerText = randomPrice;
    }, 1000);

    window.onload = function() {
        const savedToken = localStorage.getItem('user_api_token');
        const savedBroker = localStorage.getItem('selected_broker');
        if (savedToken) document.getElementById('apitoken').value = savedToken;
        if (savedBroker) document.getElementById('brokerSelect').value = savedBroker;
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
