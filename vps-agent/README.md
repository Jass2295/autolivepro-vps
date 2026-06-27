# AutoLive Pro — VPS Streaming Agent

Node.js ਨੇ Oracle VPS ਤੇ ਚੱਲਦਾ ਹੈ। Backend ਤੋਂ commands ਲੈ ਕੇ FFmpeg streaming processes manage ਕਰਦਾ ਹੈ।

## Features

- 🎬 FFmpeg stream management (YouTube, Facebook, Custom RTMP)
- 📊 Live dashboard — CPU, RAM, DISK, NETWORK stats
- ⌨️ Web Console — browser ਵਿੱਚੋਂ commands ਚਲਾਓ (PIN protected)
- 🔄 Auto-restart with exponential backoff
- 🔒 Circuit breaker (max restarts)
- 🛡️ Timing-safe API key auth

## Security

- API key fail-closed (.env ਵਿੱਚ API_KEY ਨਹੀਂ → server ਸ਼ੁਰੂ ਨਹੀਂ ਹੋਵੇਗਾ)
- Console PIN ਵੱਖਰਾ (CONSOLE_PIN env var)
- Timing-safe comparison
- Dangerous command blocking (dd, mkfs, rm -rf /)
- 30s command timeout

## Quick Deploy

```bash
git clone https://github.com/Jass2295/autolivepro-vps.git
cd autolivepro-vps/vps-agent

cat > .env << 'EOF'
PORT=5001
API_KEY=your-backend-api-key
CONSOLE_PIN=your-console-pin
MAX_STREAMS=5
MAX_RESTARTS=10
EOF

bash setup.sh
```

## URLs

| URL | Description |
|-----|-------------|
| `http://VPS_IP:5001/` | Dashboard — stats + active streams |
| `http://VPS_IP:5001/terminal` | Web Console (PIN required) |
| `http://VPS_IP:5001/health` | Health check (public) |
| `http://VPS_IP:5001/stats` | Stats JSON (public) |

## API (Backend ਤੋਂ)

All routes require `x-api-key` header.

```
POST /stream/start   — FFmpeg ਸ਼ੁਰੂ ਕਰੋ
POST /stream/stop    — FFmpeg ਬੰਦ ਕਰੋ
GET  /stream/status  — ਸਾਰੇ streams ਦਾ status
```

## Update (VPS ਤੇ)

```bash
cd ~/autolivepro-vps/vps-agent
git pull
pm2 restart autolive-agent
```
