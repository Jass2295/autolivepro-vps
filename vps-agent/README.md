# AutoLive Pro — VPS Streaming Agent

Node.js server ਜੋ Oracle VPS ਤੇ ਚੱਲਦਾ ਹੈ।
Backend ਤੋਂ commands ਲੈ ਕੇ FFmpeg streaming processes manage ਕਰਦਾ ਹੈ।

## Security
- API key fail-closed (ਜੇ .env ਵਿੱਚ API_KEY ਨਹੀਂ → server ਸ਼ੁਰੂ ਨਹੀਂ ਹੋਵੇਗਾ)
- Timing-safe key comparison
- Circuit breaker (MAX_RESTARTS ਤੋਂ ਬਾਅਦ stream ਬੰਦ)
- Stale entries automatic cleanup (capacity leak ਨਹੀਂ)

## API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Server alive check |
| GET | `/stream/status` | ਸਾਰੇ active streams |
| POST | `/stream/start` | Stream ਸ਼ੁਰੂ ਕਰੋ |
| POST | `/stream/stop` | Stream ਬੰਦ ਕਰੋ |

## .env

```
PORT=5001
API_KEY=jsk@1984
MAX_STREAMS=5
MAX_RESTARTS=10
```

## Platform RTMP URLs

| Platform | URL |
|----------|-----|
| YouTube | `rtmp://a.rtmp.youtube.com/live2/{stream_key}` |
| Facebook | `rtmps://live-api-s.facebook.com:443/rtmp/{stream_key}` |
| Custom | stream_key ਹੀ full RTMP URL |

## PM2 Commands

```bash
pm2 status
pm2 logs autolive-agent
pm2 restart autolive-agent
pm2 stop autolive-agent
```
