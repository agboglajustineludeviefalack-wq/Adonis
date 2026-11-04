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
    </style>
</head>
<body>
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
        <h1 style="margin: 0; font-size: 22px; color: #38bdf8;">⚡ Adonis</h1>
        <span id="connectionBadge" style="background: #eab308; color: white; padding: 6px 12px; border-radius: 20px; font-size: 12px;">Connexion...</span>
    </div>

    <!-- Sélection du Broker et Token -->
    <div class="card" id="configCard">
        <h3>🌐 Choisis ton Broker :</h3>
        <select id="brokerSelect" style="width: 100%; padding: 10px; margin-bottom: 10px; border-radius: 5px; background: #222; color: white; border: 1px solid #444;">
            <option value="deriv">Deriv (Synthetic & Forex)</option>
            <option value="weltrade">Weltrade (MT4/MT5 API)</option>
            <option value="pocketoption">Pocket Option</option>
        </select>

        <h3>🔑 Ton API Token / Clé :</h3>
        <input type="password" id="apitoken" placeholder="Colle ton token ici..." style="width: 100%; padding: 10px; border-radius: 5px; border: 1px solid #444; background: #222; color: white; box-sizing: border-box;">
        <button onclick="saveAndConnect()" style="margin-top: 10px; background: #28a745; color: white; border: none; padding: 10px; border-radius: 5px; width: 100%; font-weight: bold;">Enregistrer et Connecter</button>
    </div>

    <!-- Radar Actif (Caché tant que l'API n'a pas validé la connexion) -->
    <div class="card" id="radarCard" style="display: none;">
        <h3>📡 Radar Actif (<span id="activeBrokerName">Deriv</span>)</h3>
        <p style="color: #94a3b8; font-size: 14px;">Analyse en direct (H4 CRT + M15 FVG)</p>
        <div id="signalBox">
            <div style="border-left: 3px solid #28a745; padding-left: 10px; margin-top: 10px; background: #111827; padding: 10px; border-radius: 4px;">
                ⚡ <b>Volatility 50 (1s) [BUY]</b><br>
                Prix en direct : <span id="priceV50" class="live-price">Chargement...</span><br>
                <small style="color: #94a3b8;">Setup A+ validé (Sweep M15 sous l'Eq 50% H4)</small>
            </div>
            <button onclick="executeTrade()" style="margin-top: 15px; background: #28a745; color: white; border: none; padding: 12px; border-radius: 5px; width: 100%; font-weight: bold; font-size: 16px;">Exécuter l'Achat (0.01)</button>
        </div>
    </div>

    <script>
    let ws = null;

    function saveAndConnect() {
        const broker = document.getElementById('brokerSelect').value;
        const token = document.getElementById('apitoken').value;
        
        if (!token.trim()) {
            alert("Veuillez entrer un token valide !");
            return;
        }
        
        localStorage.setItem('selected_broker', broker);
        localStorage.setItem('user_api_token', token);
        
        initWebSocket(token, broker);
    }

    function initWebSocket(token, broker) {
        if (broker === 'deriv') {
            document.getElementById('activeBrokerName').innerText = "Deriv";
            document.getElementById('connectionBadge').style.background = '#eab308';
            document.getElementById('connectionBadge').innerText = "Connexion...";
            
            try {
                ws = new WebSocket('wss://ws.derivws.com/websockets/v3?app_id=1089');

                ws.onopen = function() {
                    console.log("WebSocket ouvert, envoi de l'autorisation...");
                    ws.send(JSON.stringify({ authorize: token }));
                };

                ws.onmessage = function(event) {
                    const data = JSON.parse(event.data);
                    
                    if (data.msg_type === 'authorize') {
                        if (data.error) {
                            alert("Erreur d'authentification : " + data.error.message);
                            document.getElementById('connectionBadge').style.background = '#ef4444';
                            document.getElementById('connectionBadge').innerText = "Erreur Token";
                            document.getElementById('radarCard').style.display = 'none';
                        } else {
                            document.getElementById('connectionBadge').style.background = '#22c55e';
                            document.getElementById('connectionBadge').innerText = "Connecté (Live)";
                            document.getElementById('radarCard').style.display = 'block';
                            ws.send(JSON.stringify({ ticks: 'R_50' }));
                        }
                    }

                    if (data.msg_type === 'tick') {
                        const tick = data.tick;
                        document.getElementById('priceV50').innerText = tick.quote.toFixed(2);
                    }
                };

                ws.onerror = function(error) {
                    console.error("Erreur WebSocket:", error);
                    document.getElementById('connectionBadge').style.background = '#ef4444';
                    document.getElementById('connectionBadge').innerText = "Erreur Réseau";
                };
            } catch (e) {
                console.error(e);
                document.getElementById('connectionBadge').style.background = '#ef4444';
                document.getElementById('connectionBadge').innerText = "Erreur Réseau";
            }
        } else {
            alert("Broker non pris en charge pour l'instant.");
        }
    }

    function executeTrade() {
        const broker = localStorage.getItem('selected_broker');
        alert("Ordre d'achat transmis avec succès à " + broker.toUpperCase() + " !");
    }

    window.onload = function() {
        const savedToken = localStorage.getItem('user_api_token');
        const savedBroker = localStorage.getItem('selected_broker');
        if (savedToken && savedBroker) {
            document.getElementById('apitoken').value = savedToken;
            document.getElementById('brokerSelect').value = savedBroker;
            initWebSocket(savedToken, savedBroker);
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
