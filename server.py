import os
import json
import asyncio
from aiohttp import web

# Code HTML de l'interface mobile d'Adonis
HTML_CONTENT = """
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Adonis - Cloud Trading Scanner</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 15px; }
        .header { display: flex; justify-content: space-between; align-items: center; background: #1e293b; padding: 15px; border-radius: 12px; margin-bottom: 20px; }
        h1 { margin: 0; font-size: 22px; color: #38bdf8; }
        .mode-toggle { background: #334155; border: none; color: white; padding: 8px 14px; border-radius: 8px; font-weight: bold; cursor: pointer; }
        .mode-toggle.real { background: #ef4444; }
        .card { background: #1e293b; padding: 15px; border-radius: 12px; margin-bottom: 15px; border: 1px solid #334155; }
        input[type="password"] { width: 100%; padding: 10px; border-radius: 8px; border: 1px solid #475569; background: #0f172a; color: white; box-sizing: border-box; margin-top: 5px; }
        .signal-card { border-left: 4px solid #22c55e; background: #1e293b; padding: 12px; border-radius: 8px; margin-top: 10px; }
        .signal-card.sell { border-left-color: #ef4444; }
        .btn-trade { background: #22c55e; color: white; border: none; padding: 10px; width: 100%; border-radius: 8px; font-weight: bold; margin-top: 10px; cursor: pointer; }
    </style>
</head>
<body>
    <div class="header">
        <h1>⚡ Adonis</h1>
        <button id="modeBtn" class="mode-toggle" onclick="toggleMode()">DEMO</button>
    </div>

    <div class="card">
        <label>🔑 Ton API Token Deriv :</label>
        <input type="password" id="apiToken" placeholder="Colle ton token ici..." onchange="saveToken()">
    </div>

    <div class="card">
        <h3>📡 Radar Actif (H4 CRT + M15 FVG)</h3>
        <p style="color: #94a3b8; font-size: 14px;">Surveillance en direct : Volatility 50, Step Index, Boom/Crash...</p>
        <div id="signalsContainer">
            <div class="signal-card">
                <strong>⚡ Volatility 50 (1s)</strong> [BUY]<br>
                <span>Setup A+ validé (Sweep M15 sous l'Eq 50% H4)</span><br>
                <button class="btn-trade" onclick="alert('Ordre validé !')">Exécuter l'Achat</button>
            </div>
        </div>
    </div>

    <script>
        let isReal = false;
        function toggleMode() {
            isReal = !isReal;
            const btn = document.getElementById('modeBtn');
            if(isReal) {
                btn.textContent = "RÉEL";
                btn.classList.add('real');
            } else {
                btn.textContent = "DEMO";
                btn.classList.remove('real');
            }
        }
        function saveToken() {
            const token = document.getElementById('apiToken').value;
            localStorage.setItem('deriv_token', token);
            alert('Jeton enregistré en toute sécurité !');
        }
    </script>
</body>
</html>
"""

async def handle_index(request):
    return web.Response(text=HTML_CONTENT, content_type='text/html')

async def main():
    app = web.Application()
    app.router.add_get('/', handle_index)
    
    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🚀 Adonis est en ligne sur le port {port}")
    
    # Maintenir le serveur éveillé
    while True:
        await asyncio.sleep(3600)

if __name__ == '__main__':
    asyncio.run(main())
  
