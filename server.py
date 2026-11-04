import os
import json
import asyncio
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
    </style>
</head>
<body>
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
        <h1 style="margin: 0; font-size: 22px; color: #38bdf8;">⚡ Adonis</h1>
        <span id="modeBadge" style="background: #334155; color: white; padding: 6px 12px; border-radius: 20px; font-size: 12px;">DEMO</span>
    </div>

    <!-- Sélection du Broker et Token -->
    <div class="card">
        <h3>🌐 Choisis ton Broker :</h3>
        <select id="brokerSelect" style="width: 100%; padding: 10px; margin-bottom: 10px; border-radius: 5px; background: #222; color: white; border: 1px solid #444;" onchange="updateBrokerPlaceholder()">
            <option value="deriv">Deriv (Synthetic & Forex)</option>
            <option value="weltrade">Weltrade (MT4/MT5 API)</option>
            <option value="pocketoption">Pocket Option</option>
        </select>

        <h3>🔑 Ton API Token / Clé :</h3>
        <input type="password" id="apitoken" placeholder="Colle ton token ici..." style="width: 100%; padding: 10px; border-radius: 5px; border: 1px solid #444; background: #222; color: white; box-sizing: border-box;">
        <button onclick="saveBrokerConfig()" style="margin-top: 10px; background: #28a745; color: white; border: none; padding: 10px; border-radius: 5px; width: 100%; font-weight: bold;">Enregistrer et Connecter</button>
    </div>

    <!-- Radar Actif (Caché par défaut tant que le token n'est pas configuré) -->
    <div class="card" id="radarCard" style="display: none;">
        <h3>📡 Radar Actif (<span id="activeBrokerName">Deriv</span>)</h3>
        <p style="color: #94a3b8; font-size: 14px;">Surveillance en direct : H4 CRT + M15 FVG...</p>
        <div id="signalBox">
            <div style="border-left: 3px solid #28a745; padding-left: 10px; margin-top: 10px;">
                ⚡ <b>Volatility 50 (1s) [BUY]</b><br>
                Setup A+ validé (Sweep M15 sous l'Eq 50% H4)
            </div>
            <button onclick="executeTrade()" style="margin-top: 15px; background: #28a745; color: white; border: none; padding: 12px; border-radius: 5px; width: 100%; font-weight: bold; font-size: 16px;">Exécuter l'Achat</button>
        </div>
    </div>

    <script>
    function updateBrokerPlaceholder() {
        const broker = document.getElementById('brokerSelect').value;
        const input = document.getElementById('apitoken');
        if (broker === 'deriv') {
            input.placeholder = "Colle ton token Deriv ici...";
        } else if (broker === 'weltrade') {
            input.placeholder = "Colle ta clé API Weltrade ici...";
        } else {
            input.placeholder = "Colle ta clé d'accès ici...";
        }
    }

    function saveBrokerConfig() {
        const broker = document.getElementById('brokerSelect').value;
        const token = document.getElementById('apitoken').value;
        
        if (!token.trim()) {
            alert("Veuillez entrer une clé ou un token valide !");
            return;
        }
        
        localStorage.setItem('selected_broker', broker);
        localStorage.setItem('user_api_token', token);
        
        document.getElementById('activeBrokerName').innerText = broker.toUpperCase();
        document.getElementById('radarCard').style.display = 'block';
        alert("Connexion configurée avec succès ! Le radar s'active.");
    }

    function executeTrade() {
        const broker = localStorage.getItem('selected_broker');
        alert("Ordre transmis à " + broker.toUpperCase() + " avec succès !");
    }

    window.onload = function() {
        const savedToken = localStorage.getItem('user_api_token');
        const savedBroker = localStorage.getItem('selected_broker');
        if (savedToken && savedBroker) {
            document.getElementById('apitoken').value = savedToken;
            document.getElementById('brokerSelect').value = savedBroker;
            document.getElementById('activeBrokerName').innerText = savedBroker.toUpperCase();
            document.getElementById('radarCard').style.display = 'block';
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
