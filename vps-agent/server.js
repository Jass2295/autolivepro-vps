require("dotenv").config();

const express      = require("express");
const http         = require("http");
const { spawn }    = require("child_process");
const crypto       = require("crypto");
const os           = require("os");
const fs           = require("fs");
const { WebSocketServer } = require("ws");

// ─── Config ──────────────────────────────────────────────────────────────────
const app          = express();
const server       = http.createServer(app);
const PORT         = Number(process.env.PORT        || 5001);
const API_KEY      = process.env.API_KEY            || "";
const CONSOLE_PIN  = process.env.CONSOLE_PIN        || API_KEY;
const MAX_STREAMS  = Number(process.env.MAX_STREAMS  || 5);
const MAX_RESTARTS = Number(process.env.MAX_RESTARTS || 10);

app.use(express.json());

if (!API_KEY) {
  console.error("FATAL: API_KEY not set in .env");
  process.exit(1);
}

function safeEqual(a, b) {
  try { return crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b)); }
  catch { return false; }
}

// ─── System Stats ─────────────────────────────────────────────────────────────
let _cpuUsage = 0;
let _net      = { rx: 0, tx: 0 };
let _prevCpu  = null;
let _prevNet  = null;
let _prevNetT = null;

function _readCpuTimes() {
  try {
    const line  = fs.readFileSync("/proc/stat", "utf8").split("\n")[0];
    const vals  = line.split(/\s+/).slice(1).map(Number);
    const idle  = vals[3];
    const total = vals.reduce((s, v) => s + v, 0);
    return { idle, total };
  } catch { return null; }
}

function _updateStats() {
  const cur = _readCpuTimes();
  if (_prevCpu && cur) {
    const di = cur.idle  - _prevCpu.idle;
    const dt = cur.total - _prevCpu.total;
    _cpuUsage = dt > 0 ? Math.round((1 - di / dt) * 1000) / 10 : 0;
  }
  _prevCpu = cur;

  try {
    let rx = 0, tx = 0;
    fs.readFileSync("/proc/net/dev", "utf8").split("\n").slice(2).forEach(l => {
      const p = l.trim().split(/\s+/);
      if (p.length < 10 || p[0].replace(":","") === "lo") return;
      rx += parseInt(p[1]) || 0;
      tx += parseInt(p[9]) || 0;
    });
    const now = Date.now();
    if (_prevNet && _prevNetT) {
      const sec = (now - _prevNetT) / 1000;
      _net.rx = Math.round((rx - _prevNet.rx) / sec);
      _net.tx = Math.round((tx - _prevNet.tx) / sec);
    }
    _prevNet  = { rx, tx };
    _prevNetT = Date.now();
  } catch {}
}

function getStats() {
  const tot  = os.totalmem();
  const free = os.freemem();
  const used = tot - free;

  let disk = { pct: 0, used_gb: 0, total_gb: 0 };
  try {
    const st = fs.statfsSync("/");
    const total_gb  = Math.round(st.blocks  * st.bsize / 1e9 * 10) / 10;
    const free_gb   = Math.round(st.bfree   * st.bsize / 1e9 * 10) / 10;
    const used_gb   = Math.round((total_gb - free_gb) * 10) / 10;
    disk = { pct: Math.round(used_gb / total_gb * 100), used_gb, total_gb };
  } catch {}

  return {
    cpu:     { pct: _cpuUsage,  cores: os.cpus().length },
    ram:     { pct: Math.round(used / tot * 100),
               used_mb: Math.round(used / 1048576),
               total_mb: Math.round(tot / 1048576) },
    disk,
    network: { rx_bps: _net.rx, tx_bps: _net.tx },
    uptime:  Math.round(os.uptime()),
  };
}

_readCpuTimes();
setInterval(_updateStats, 2000);

// ─── Stream State ─────────────────────────────────────────────────────────────
const streams = new Map();

function rtmpUrl(platform, streamKey) {
  if (platform === "youtube")  return `rtmp://a.rtmp.youtube.com/live2/${streamKey}`;
  if (platform === "facebook") return `rtmps://live-api-s.facebook.com:443/rtmp/${streamKey}`;
  return streamKey;
}

function buildFfmpegArgs(cfg) {
  const args = ["-hide_banner", "-loglevel", "warning", "-re"];
  if (cfg.loop) args.push("-stream_loop", "-1");
  args.push("-i", cfg.video_url, "-c:v", "copy", "-c:a", "copy",
            "-f", "flv", rtmpUrl(cfg.platform, cfg.stream_key));
  return args;
}

function cleanupEntry(slotId) {
  const e = streams.get(slotId);
  if (!e) return;
  if (e.stopTimer) { clearTimeout(e.stopTimer); e.stopTimer = null; }
  e.proc = null;
  streams.delete(slotId);
}

function startFfmpeg(slotId, cfg, restartCount = 0) {
  const entry = streams.get(slotId);
  if (!entry) return;

  const proc = spawn("ffmpeg", buildFfmpegArgs(cfg), { stdio: ["ignore","pipe","pipe"] });

  proc.on("error", err => {
    console.error(`[${slotId}] spawn error: ${err.message}`);
    const cur = streams.get(slotId);
    if (!cur || cur.proc !== proc) return;
    cur.status = "error";
    cleanupEntry(slotId);
  });

  proc.stdout.on("data", d => process.stdout.write(`[${slotId}] ${d}`));
  proc.stderr.on("data", d => process.stderr.write(`[${slotId}] ${d}`));

  entry.proc         = proc;
  entry.startedAt    = new Date().toISOString();
  entry.restartCount = restartCount;
  entry.status       = "live";

  if (cfg.stop_after_min > 0) {
    if (entry.stopTimer) clearTimeout(entry.stopTimer);
    entry.stopTimer = setTimeout(() => stopStream(slotId),
                                  cfg.stop_after_min * 60000);
  }

  proc.on("exit", (code, signal) => {
    console.log(`[${slotId}] exit code=${code} signal=${signal}`);
    const cur = streams.get(slotId);
    if (!cur || cur.proc !== proc) return;
    const manual = signal === "SIGTERM" || signal === "SIGKILL" || cur.status === "stopped";
    if (!manual && cfg.loop) {
      if (restartCount >= MAX_RESTARTS) {
        cur.status = "error"; cleanupEntry(slotId); return;
      }
      const delay = Math.min(5000 * (restartCount + 1), 30000);
      cur.status = "restarting"; cur.proc = null;
      setTimeout(() => {
        const still = streams.get(slotId);
        if (still && still.status === "restarting") startFfmpeg(slotId, cfg, restartCount + 1);
      }, delay);
    } else {
      cleanupEntry(slotId);
    }
  });
}

function stopStream(slotId) {
  const e = streams.get(slotId);
  if (!e) return false;
  e.status = "stopped";
  const proc = e.proc;
  cleanupEntry(slotId);
  if (proc) {
    try { proc.kill("SIGTERM"); } catch {}
    setTimeout(() => { try { proc.kill("SIGKILL"); } catch {} }, 5000);
  }
  return true;
}

// ─── Dashboard HTML ───────────────────────────────────────────────────────────
function dashboardHTML() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AutoLive Pro — VPS Panel</title>
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  :root{
    --bg:#08080f;--card:#10101c;--card2:#141420;
    --border:rgba(255,45,95,0.18);--accent:#ff2d5f;
    --accent2:#ff6b8a;--text:#e8e8f0;--muted:#6b6b88;
    --green:#22c55e;--yellow:#f59e0b;--red:#ef4444;
  }
  body{background:var(--bg);color:var(--text);font-family:'Inter',system-ui,sans-serif;min-height:100vh}
  header{
    display:flex;align-items:center;justify-content:space-between;
    padding:16px 28px;border-bottom:1px solid var(--border);
    background:rgba(10,10,20,0.95);position:sticky;top:0;z-index:10;
    backdrop-filter:blur(12px);
  }
  .logo{display:flex;align-items:center;gap:10px}
  .logo svg{width:32px;height:32px}
  .logo-text{font-size:15px;font-weight:700;letter-spacing:.5px}
  .logo-text span{color:var(--accent)}
  .logo-sub{font-size:10px;color:var(--muted);letter-spacing:2px;text-transform:uppercase;margin-top:1px}
  .badge{
    background:rgba(255,45,95,0.12);color:var(--accent);
    border:1px solid rgba(255,45,95,0.3);border-radius:20px;
    padding:4px 12px;font-size:12px;font-weight:600;
    display:flex;align-items:center;gap:6px;
  }
  .dot{width:7px;height:7px;border-radius:50%;background:var(--green);
       box-shadow:0 0 6px var(--green);animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
  .main{padding:24px 28px;max-width:1200px;margin:0 auto}
  h2{font-size:22px;font-weight:700;margin-bottom:4px}
  .sub{color:var(--muted);font-size:13px;margin-bottom:24px}
  .stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:16px;margin-bottom:28px}
  .card{
    background:var(--card);border:1px solid var(--border);border-radius:14px;
    padding:20px 22px;transition:border-color .2s;
  }
  .card:hover{border-color:rgba(255,45,95,0.4)}
  .card-label{font-size:11px;letter-spacing:1.5px;text-transform:uppercase;color:var(--muted);margin-bottom:10px}
  .card-value{font-size:32px;font-weight:800;color:var(--accent);line-height:1;margin-bottom:6px}
  .card-detail{font-size:12px;color:var(--muted)}
  .bar-bg{height:4px;background:rgba(255,255,255,0.06);border-radius:4px;margin-top:12px;overflow:hidden}
  .bar-fill{height:100%;border-radius:4px;background:linear-gradient(90deg,var(--accent),var(--accent2));transition:width .6s ease}
  .section-title{font-size:14px;font-weight:700;letter-spacing:.5px;margin-bottom:14px;
    display:flex;align-items:center;gap:8px;}
  .section-title::after{content:'';flex:1;height:1px;background:var(--border)}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th{text-align:left;padding:10px 14px;color:var(--muted);font-size:11px;
     letter-spacing:1px;text-transform:uppercase;border-bottom:1px solid var(--border)}
  td{padding:12px 14px;border-bottom:1px solid rgba(255,255,255,0.04)}
  tr:last-child td{border-bottom:none}
  tr:hover td{background:rgba(255,45,95,0.04)}
  .pill{display:inline-flex;align-items:center;gap:5px;padding:3px 10px;
        border-radius:20px;font-size:11px;font-weight:600}
  .pill.live{background:rgba(34,197,94,.15);color:var(--green)}
  .pill.restarting{background:rgba(245,158,11,.15);color:var(--yellow)}
  .pill.error{background:rgba(239,68,68,.15);color:var(--red)}
  .pill.starting{background:rgba(99,102,241,.15);color:#818cf8}
  .empty{text-align:center;padding:40px;color:var(--muted);font-size:13px}
  .empty span{display:block;font-size:28px;margin-bottom:8px}
  .nav-btn{
    text-decoration:none;background:rgba(255,45,95,0.1);color:var(--accent);
    border:1px solid rgba(255,45,95,0.3);border-radius:8px;
    padding:8px 16px;font-size:13px;font-weight:600;
    transition:all .2s;display:flex;align-items:center;gap:6px;
  }
  .nav-btn:hover{background:rgba(255,45,95,0.2)}
  .refresh{font-size:11px;color:var(--muted);margin-top:16px;text-align:right}
  .net-row{display:flex;gap:16px}
  .net-item{flex:1}
  .net-label{font-size:10px;color:var(--muted);margin-bottom:2px}
  .net-val{font-size:18px;font-weight:700;color:var(--accent)}
</style>
</head>
<body>
<header>
  <div class="logo">
    <svg viewBox="0 0 32 32" fill="none">
      <circle cx="16" cy="16" r="15" fill="rgba(255,45,95,0.12)" stroke="rgba(255,45,95,0.4)" stroke-width="1.5"/>
      <circle cx="16" cy="16" r="6" fill="#ff2d5f"/>
      <path d="M16 4 A12 12 0 0 1 28 16" stroke="#ff2d5f" stroke-width="2" stroke-linecap="round" opacity=".4"/>
      <path d="M16 28 A12 12 0 0 1 4 16" stroke="#ff2d5f" stroke-width="2" stroke-linecap="round" opacity=".4"/>
    </svg>
    <div>
      <div class="logo-text">AutoLive <span>Pro</span></div>
      <div class="logo-sub">VPS Panel</div>
    </div>
  </div>
  <div style="display:flex;align-items:center;gap:12px">
    <a href="/terminal" class="nav-btn">⌨ Console</a>
    <div class="badge"><div class="dot"></div>Live</div>
  </div>
</header>

<div class="main">
  <h2>Dashboard</h2>
  <div class="sub">Live system stats and active streams</div>

  <div class="stats-grid">
    <div class="card">
      <div class="card-label">CPU Load</div>
      <div class="card-value" id="cpu-pct">—</div>
      <div class="card-detail" id="cpu-cores">loading...</div>
      <div class="bar-bg"><div class="bar-fill" id="cpu-bar" style="width:0%"></div></div>
    </div>
    <div class="card">
      <div class="card-label">RAM</div>
      <div class="card-value" id="ram-pct">—</div>
      <div class="card-detail" id="ram-detail">loading...</div>
      <div class="bar-bg"><div class="bar-fill" id="ram-bar" style="width:0%"></div></div>
    </div>
    <div class="card">
      <div class="card-label">Disk</div>
      <div class="card-value" id="disk-pct">—</div>
      <div class="card-detail" id="disk-detail">loading...</div>
      <div class="bar-bg"><div class="bar-fill" id="disk-bar" style="width:0%"></div></div>
    </div>
    <div class="card">
      <div class="card-label">Network</div>
      <div class="net-row" style="margin-top:4px">
        <div class="net-item">
          <div class="net-label">↓ Download</div>
          <div class="net-val" id="net-rx">—</div>
        </div>
        <div class="net-item">
          <div class="net-label">↑ Upload</div>
          <div class="net-val" id="net-tx">—</div>
        </div>
      </div>
      <div class="card-detail" style="margin-top:10px" id="net-total">—</div>
    </div>
    <div class="card">
      <div class="card-label">Active Streams</div>
      <div class="card-value" id="stream-count">—</div>
      <div class="card-detail" id="stream-max">loading...</div>
      <div class="bar-bg"><div class="bar-fill" id="stream-bar" style="width:0%"></div></div>
    </div>
    <div class="card">
      <div class="card-label">VPS Uptime</div>
      <div class="card-value" id="uptime-val">—</div>
      <div class="card-detail" id="uptime-detail">loading...</div>
    </div>
  </div>

  <div class="section-title">Active Streams</div>
  <div class="card" style="padding:0;overflow:hidden">
    <div id="streams-body">
      <div class="empty"><span>📡</span>No active streams</div>
    </div>
  </div>

  <div class="refresh" id="refresh-ts">Refreshing every 3s...</div>
</div>

<script>
function fmt(bytes) {
  if (bytes < 1024) return bytes + ' B/s';
  if (bytes < 1048576) return (bytes/1024).toFixed(1) + ' KB/s';
  return (bytes/1048576).toFixed(2) + ' MB/s';
}
function fmtUptime(sec) {
  const d = Math.floor(sec/86400), h = Math.floor((sec%86400)/3600),
        m = Math.floor((sec%3600)/60);
  if (d) return d + 'd ' + h + 'h';
  if (h) return h + 'h ' + m + 'm';
  return m + 'm ' + (sec%60) + 's';
}
function bar(id, pct) {
  const el = document.getElementById(id);
  if (el) el.style.width = Math.min(pct,100) + '%';
}

async function refresh() {
  try {
    const [sys, st] = await Promise.all([
      fetch('/stats').then(r=>r.json()),
      fetch('/stream/status').then(r=>r.json()).catch(()=>({streams:[]}))
    ]);

    document.getElementById('cpu-pct').textContent    = sys.cpu.pct + '%';
    document.getElementById('cpu-cores').textContent  = sys.cpu.cores + ' cores';
    bar('cpu-bar', sys.cpu.pct);

    document.getElementById('ram-pct').textContent    = sys.ram.pct + '%';
    document.getElementById('ram-detail').textContent = sys.ram.used_mb + ' MB / ' + sys.ram.total_mb + ' MB';
    bar('ram-bar', sys.ram.pct);

    document.getElementById('disk-pct').textContent   = sys.disk.pct + '%';
    document.getElementById('disk-detail').textContent= sys.disk.used_gb + ' GB / ' + sys.disk.total_gb + ' GB';
    bar('disk-bar', sys.disk.pct);

    document.getElementById('net-rx').textContent = fmt(sys.network.rx_bps);
    document.getElementById('net-tx').textContent = fmt(sys.network.tx_bps);
    document.getElementById('net-total').textContent = 'Total: ↓' + fmt(sys.network.rx_bps) + ' ↑' + fmt(sys.network.tx_bps);

    document.getElementById('uptime-val').textContent   = fmtUptime(sys.uptime);
    document.getElementById('uptime-detail').textContent= 'System online';

    const sl = (st.streams||[]);
    document.getElementById('stream-count').textContent= sl.length;
    document.getElementById('stream-max').textContent  = 'of ' + (st.max||5) + ' max slots';
    bar('stream-bar', sl.length / (st.max||5) * 100);

    const body = document.getElementById('streams-body');
    if (!sl.length) {
      body.innerHTML = '<div class="empty"><span>📡</span>No active streams</div>';
    } else {
      body.innerHTML = '<table><thead><tr>' +
        '<th>Slot</th><th>Platform</th><th>Status</th><th>Restarts</th><th>Started</th>' +
        '</tr></thead><tbody>' +
        sl.map(s => {
          const d = new Date(s.started_at);
          const ago = Math.round((Date.now()-d)/60000);
          return '<tr>' +
            '<td><b>' + s.slot_id + '</b></td>' +
            '<td>' + (s.platform||'—') + '</td>' +
            '<td><span class="pill ' + s.status + '">' + s.status + '</span></td>' +
            '<td>' + (s.restart_count||0) + '</td>' +
            '<td>' + (ago < 1 ? 'just now' : ago + 'm ago') + '</td>' +
            '</tr>';
        }).join('') +
        '</tbody></table>';
    }

    document.getElementById('refresh-ts').textContent =
      'Last updated: ' + new Date().toLocaleTimeString();
  } catch(e) {
    document.getElementById('refresh-ts').textContent = 'Update failed — ' + e.message;
  }
}

refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>`;
}

// ─── Terminal HTML ─────────────────────────────────────────────────────────────
function terminalHTML() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AutoLive Pro — Console</title>
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  :root{
    --bg:#08080f;--card:#10101c;--border:rgba(255,45,95,0.18);
    --accent:#ff2d5f;--text:#e8e8f0;--muted:#6b6b88;--green:#22c55e;
  }
  body{background:var(--bg);color:var(--text);font-family:system-ui,sans-serif;
       height:100vh;display:flex;flex-direction:column}
  header{
    display:flex;align-items:center;justify-content:space-between;
    padding:12px 20px;border-bottom:1px solid var(--border);
    background:rgba(10,10,20,0.97);flex-shrink:0;
  }
  .logo-text{font-size:14px;font-weight:700}
  .logo-text span{color:var(--accent)}
  .logo-sub{font-size:10px;color:var(--muted);letter-spacing:2px;text-transform:uppercase}
  .back{
    text-decoration:none;color:var(--muted);font-size:13px;
    display:flex;align-items:center;gap:6px;transition:color .2s;
  }
  .back:hover{color:var(--text)}

  /* ── PIN screen ── */
  #pin-screen{
    flex:1;display:flex;align-items:center;justify-content:center;
  }
  .pin-box{
    background:var(--card);border:1px solid var(--border);border-radius:16px;
    padding:40px;width:340px;text-align:center;
  }
  .pin-icon{font-size:40px;margin-bottom:16px}
  .pin-box h3{font-size:18px;margin-bottom:6px}
  .pin-box p{font-size:13px;color:var(--muted);margin-bottom:24px}
  .pin-input{
    width:100%;background:#0a0a16;border:1px solid var(--border);
    color:var(--text);padding:12px 16px;border-radius:10px;
    font-size:16px;text-align:center;letter-spacing:4px;outline:none;
    transition:border-color .2s;margin-bottom:14px;
  }
  .pin-input:focus{border-color:var(--accent)}
  .pin-btn{
    width:100%;background:var(--accent);color:#fff;border:none;
    padding:12px;border-radius:10px;font-size:14px;font-weight:700;
    cursor:pointer;transition:opacity .2s;
  }
  .pin-btn:hover{opacity:.85}
  .pin-err{color:#ef4444;font-size:12px;margin-top:8px;display:none}

  /* ── Terminal screen ── */
  #term-screen{flex:1;display:none;flex-direction:column;overflow:hidden}
  .term-bar{
    display:flex;align-items:center;justify-content:space-between;
    padding:8px 16px;background:#0a0a16;border-bottom:1px solid var(--border);
    flex-shrink:0;font-size:12px;
  }
  .term-info{color:var(--muted)}
  .term-info b{color:var(--green)}
  .quick-cmds{display:flex;gap:6px;flex-wrap:wrap}
  .qbtn{
    background:rgba(255,45,95,0.1);color:var(--accent);border:1px solid rgba(255,45,95,0.25);
    border-radius:6px;padding:3px 10px;font-size:11px;cursor:pointer;
    font-family:monospace;transition:background .2s;white-space:nowrap;
  }
  .qbtn:hover{background:rgba(255,45,95,0.22)}
  #output{
    flex:1;overflow-y:auto;padding:16px;
    font-family:'Fira Code','Courier New',monospace;font-size:13px;line-height:1.6;
    background:#06060e;
  }
  #output .line{word-break:break-all}
  #output .line.err{color:#f87171}
  #output .line.sys{color:var(--muted);font-style:italic}
  #output .line.ok{color:var(--green)}
  #output .line.cmd{color:var(--accent);font-weight:600}
  .input-row{
    display:flex;align-items:center;gap:0;
    border-top:1px solid var(--border);background:#0a0a16;flex-shrink:0;
  }
  .prompt{padding:12px 8px 12px 16px;color:var(--accent);
          font-family:monospace;font-size:13px;flex-shrink:0}
  #cmd-input{
    flex:1;background:transparent;border:none;color:var(--text);
    font-family:'Fira Code','Courier New',monospace;font-size:13px;
    padding:12px 8px;outline:none;
  }
  .run-btn{
    background:var(--accent);color:#fff;border:none;
    padding:12px 20px;font-size:13px;font-weight:700;cursor:pointer;
    transition:opacity .2s;flex-shrink:0;
  }
  .run-btn:hover{opacity:.8}
  .run-btn:disabled{opacity:.4;cursor:default}
</style>
</head>
<body>
<header>
  <div>
    <div class="logo-text">AutoLive <span>Pro</span> — Console</div>
    <div class="logo-sub">VPS Terminal</div>
  </div>
  <a href="/" class="back">← Dashboard</a>
</header>

<!-- PIN Screen -->
<div id="pin-screen">
  <div class="pin-box">
    <div class="pin-icon">🔒</div>
    <h3>Console Access</h3>
    <p>Enter your Console PIN to continue</p>
    <input id="pin-input" class="pin-input" type="password"
           placeholder="• • • • • • • •" maxlength="64"
           autocomplete="current-password">
    <button class="pin-btn" onclick="unlock()">Unlock Console</button>
    <div class="pin-err" id="pin-err">Wrong PIN. Try again.</div>
  </div>
</div>

<!-- Terminal Screen -->
<div id="term-screen">
  <div class="term-bar">
    <div class="term-info">Connected to <b>VPS</b> — Mumbai</div>
    <div class="quick-cmds">
      <button class="qbtn" onclick="run('pm2 status')">pm2 status</button>
      <button class="qbtn" onclick="run('pm2 logs autolive-agent --lines 30 --nostream')">pm2 logs</button>
      <button class="qbtn" onclick="run('pm2 restart autolive-agent')">pm2 restart</button>
      <button class="qbtn" onclick="run('free -h')">free -h</button>
      <button class="qbtn" onclick="run('df -h /')">df -h</button>
      <button class="qbtn" onclick="run('uptime')">uptime</button>
      <button class="qbtn" onclick="run('cd ~/autolivepro-vps/vps-agent && git pull && pm2 restart autolive-agent')">git pull</button>
      <button class="qbtn" onclick="clearOutput()">clear</button>
    </div>
  </div>

  <div id="output">
    <div class="line sys">── AutoLive Pro Console ready. Type a command below. ──</div>
  </div>

  <div class="input-row">
    <div class="prompt">ubuntu@vps:~$</div>
    <input id="cmd-input" placeholder="Enter command..." onkeydown="onKey(event)">
    <button class="run-btn" id="run-btn" onclick="runInput()">Run</button>
  </div>
</div>

<script>
let pin = '';
let running = false;
const hist = [];
let histIdx = -1;

document.getElementById('pin-input').addEventListener('keydown', e => {
  if (e.key === 'Enter') unlock();
});

async function unlock() {
  const p = document.getElementById('pin-input').value.trim();
  if (!p) return;
  const res = await fetch('/terminal/auth', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({pin: p})
  });
  const data = await res.json();
  if (data.ok) {
    pin = p;
    document.getElementById('pin-screen').style.display='none';
    document.getElementById('term-screen').style.display='flex';
    document.getElementById('cmd-input').focus();
  } else {
    const err = document.getElementById('pin-err');
    err.style.display='block';
    document.getElementById('pin-input').value='';
    setTimeout(()=>err.style.display='none', 3000);
  }
}

function addLine(text, cls='') {
  const out = document.getElementById('output');
  text.split('\\n').forEach(line => {
    if (!line && text.split('\\n').length === 1) return;
    const d = document.createElement('div');
    d.className = 'line ' + cls;
    d.textContent = line;
    out.appendChild(d);
  });
  out.scrollTop = out.scrollHeight;
}

function clearOutput() {
  document.getElementById('output').innerHTML =
    '<div class="line sys">── Cleared ──</div>';
}

async function run(cmd) {
  if (running || !cmd.trim()) return;
  running = true;
  hist.unshift(cmd); histIdx = -1;
  document.getElementById('cmd-input').value = '';
  document.getElementById('run-btn').disabled = true;

  addLine('$ ' + cmd, 'cmd');

  try {
    const res = await fetch('/terminal/run', {
      method:'POST',
      headers:{'Content-Type':'application/json','x-console-pin':pin},
      body: JSON.stringify({cmd})
    });
    const data = await res.json();
    if (data.stdout) addLine(data.stdout.trimEnd(), 'ok');
    if (data.stderr) addLine(data.stderr.trimEnd(), 'err');
    if (!data.ok) addLine('Exit code: ' + data.code, 'err');
  } catch(e) {
    addLine('Request failed: ' + e.message, 'err');
  }

  addLine('', 'sys');
  running = false;
  document.getElementById('run-btn').disabled = false;
  document.getElementById('cmd-input').focus();
}

function runInput() {
  run(document.getElementById('cmd-input').value.trim());
}

function onKey(e) {
  if (e.key === 'Enter')     { runInput(); return; }
  if (e.key === 'ArrowUp')   { histIdx = Math.min(histIdx+1, hist.length-1); e.target.value = hist[histIdx]||''; }
  if (e.key === 'ArrowDown') { histIdx = Math.max(histIdx-1, -1);            e.target.value = histIdx<0?'':hist[histIdx]; }
}
</script>
</body>
</html>`;
}

// ─── Public Routes ────────────────────────────────────────────────────────────
app.get("/", (_req, res) => res.send(dashboardHTML()));
app.get("/terminal", (_req, res) => res.send(terminalHTML()));

app.get("/health", (_req, res) =>
  res.json({ ok: true, time: new Date().toISOString(), active_streams: streams.size }));

app.get("/stats", (_req, res) => {
  const s = getStats();
  const list = [];
  for (const [slot_id, e] of streams)
    list.push({ slot_id, status: e.status, platform: e.config.platform,
                restart_count: e.restartCount, started_at: e.startedAt });
  res.json({ ...s, streams: { active: streams.size, max: MAX_STREAMS, list } });
});

// PIN auth endpoint
app.post("/terminal/auth", (req, res) => {
  const { pin } = req.body || {};
  if (!pin || !safeEqual(String(pin), CONSOLE_PIN))
    return res.status(401).json({ ok: false, error: "Wrong PIN" });
  res.json({ ok: true });
});

// ─── Console PIN middleware ───────────────────────────────────────────────────
function requirePin(req, res, next) {
  const pin = req.headers["x-console-pin"] || "";
  if (safeEqual(pin, CONSOLE_PIN)) return next();
  res.status(401).json({ ok: false, error: "Wrong PIN" });
}

// Command runner — PIN protected, 30s timeout
app.post("/terminal/run", requirePin, (req, res) => {
  const { cmd } = req.body || {};
  if (!cmd || typeof cmd !== "string")
    return res.status(400).json({ ok: false, error: "cmd required" });

  // Block obviously dangerous patterns
  const blocked = /(\bdd\b.*of=\/dev|\bmkfs\b|\brm\s+-rf\s+\/\b)/i;
  if (blocked.test(cmd))
    return res.status(403).json({ ok: false, error: "Command blocked for safety" });

  let stdout = "", stderr = "", killed = false;
  const proc = spawn("bash", ["-c", cmd], { env: { ...process.env, HOME: "/home/ubuntu" } });

  const timer = setTimeout(() => {
    killed = true;
    try { proc.kill("SIGKILL"); } catch {}
  }, 30000);

  proc.stdout.on("data", d => { stdout += d; });
  proc.stderr.on("data", d => { stderr += d; });

  proc.on("close", code => {
    clearTimeout(timer);
    res.json({ ok: code === 0, code, stdout, stderr: killed ? stderr + "\n[Killed: 30s timeout]" : stderr });
  });

  proc.on("error", err => {
    clearTimeout(timer);
    res.json({ ok: false, code: -1, stdout: "", stderr: err.message });
  });
});

// ─── API Key protected routes ─────────────────────────────────────────────────
app.use((req, res, next) => {
  const key = req.headers["x-api-key"] || "";
  if (safeEqual(key, API_KEY)) return next();
  res.status(401).json({ ok: false, error: "Unauthorized" });
});

app.get("/stream/status", (_req, res) => {
  const list = [];
  for (const [slot_id, e] of streams)
    list.push({ slot_id, status: e.status, platform: e.config.platform,
                loop: e.config.loop, stop_after_min: e.config.stop_after_min,
                started_at: e.startedAt, restart_count: e.restartCount,
                pid: e.proc ? e.proc.pid : null });
  res.json({ ok: true, active: list.length, max: MAX_STREAMS, streams: list });
});

app.post("/stream/start", (req, res) => {
  const { slot_id, video_url, platform, stream_key,
          loop = true, stop_after_min = 0 } = req.body || {};

  if (!slot_id || !video_url || !platform || !stream_key)
    return res.status(400).json({ ok: false, error: "slot_id, video_url, platform, stream_key required" });

  if (!["youtube","facebook","custom"].includes(platform))
    return res.status(400).json({ ok: false, error: "platform must be youtube, facebook or custom" });

  if (streams.has(slot_id)) stopStream(slot_id);

  if (streams.size >= MAX_STREAMS)
    return res.status(429).json({ ok: false, error: `VPS at capacity (max ${MAX_STREAMS})` });

  const cfg = { video_url, platform, stream_key, loop, stop_after_min };
  streams.set(slot_id, { proc: null, config: cfg, status: "starting",
    startedAt: new Date().toISOString(), restartCount: 0, stopTimer: null });

  startFfmpeg(slot_id, cfg, 0);
  res.json({ ok: true, slot_id, message: "Stream starting — going live in ~10s" });
});

app.post("/stream/stop", (req, res) => {
  const { slot_id } = req.body || {};
  if (!slot_id) return res.status(400).json({ ok: false, error: "slot_id required" });
  res.json({ ok: true, stopped: stopStream(slot_id), slot_id });
});

// ─── Start ────────────────────────────────────────────────────────────────────
server.listen(PORT, "0.0.0.0", () => {
  console.log(`\n🎬  AutoLive Pro VPS Agent — port ${PORT}`);
  console.log(`   Dashboard : http://localhost:${PORT}/`);
  console.log(`   Console   : http://localhost:${PORT}/terminal`);
  console.log(`   Max streams: ${MAX_STREAMS} | Restarts: ${MAX_RESTARTS}\n`);
});

process.on("SIGTERM", () => {
  for (const id of [...streams.keys()]) stopStream(id);
  process.exit(0);
});
