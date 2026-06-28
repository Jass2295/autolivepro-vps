require("dotenv").config();
const express  = require("express");
const http     = require("http");
const { WebSocketServer } = require("ws");
const { spawn, execSync } = require("child_process");
const fs       = require("fs");
const path     = require("path");
const os       = require("os");
const multer   = require("multer");
const crypto   = require("crypto");

const app    = express();
const server = http.createServer(app);

const PORT        = Number(process.env.PORT || 5001);
const API_KEY     = process.env.API_KEY || "changeme";
const CONSOLE_PIN = String(process.env.CONSOLE_PIN || "1234");
const MAX_STREAMS = Number(process.env.MAX_STREAMS || 15);

app.use(express.json({ limit: "10mb" }));

function safeEqual(a, b) {
  try {
    const ba = Buffer.from(String(a).padEnd(64));
    const bb = Buffer.from(String(b).padEnd(64));
    return crypto.timingSafeEqual(ba, bb) && a.length === b.length;
  } catch { return false; }
}

const streams = new Map();

let _prevNet = null, _prevNetTime = 0;
function getNetStats() {
  try {
    const raw = fs.readFileSync("/proc/net/dev", "utf8");
    let rx = 0, tx = 0;
    for (const line of raw.split("\n").slice(2)) {
      const p = line.trim().split(/\s+/);
      if (p.length >= 10 && !p[0].startsWith("lo")) { rx += Number(p[1]); tx += Number(p[9]); }
    }
    const now = Date.now();
    let rx_bps = 0, tx_bps = 0;
    if (_prevNet && now - _prevNetTime > 0) {
      const dt = (now - _prevNetTime) / 1000;
      rx_bps = Math.max(0, (rx - _prevNet.rx) / dt);
      tx_bps = Math.max(0, (tx - _prevNet.tx) / dt);
    }
    _prevNet = { rx, tx }; _prevNetTime = now;
    return { rx_bps, tx_bps };
  } catch { return { rx_bps: 0, tx_bps: 0 }; }
}

function getStats() {
  let cpuPct = 0, cores = os.cpus().length;
  try {
    const load = os.loadavg()[0];
    cpuPct = Math.min(100, Math.round((load / cores) * 100));
  } catch {}

  const totalMem = os.totalmem(), freeMem = os.freemem();
  const usedMem  = totalMem - freeMem;

  let diskUsed = 0, diskTotal = 50;
  try {
    const df = execSync("df -BG / 2>/dev/null | tail -1").toString().trim().split(/\s+/);
    diskTotal = parseInt(df[1]) || 50;
    diskUsed  = parseInt(df[2]) || 0;
  } catch {}

  const net = getNetStats();

  return {
    cpu:          { pct: cpuPct, cores },
    ram:          { pct: Math.round((usedMem/totalMem)*100), used_mb: Math.round(usedMem/1024/1024), total_mb: Math.round(totalMem/1024/1024) },
    disk:         { pct: diskTotal ? Math.round((diskUsed/diskTotal)*100) : 0, used_gb: diskUsed, total_gb: diskTotal },
    network:      net,
    uptime:       Math.floor(os.uptime()),
    streams:      { list: [...streams.values()].map(e => ({
      slot_id: e.cfg.slot_id, platform: e.cfg.platform, status: e.status,
      started_at: e.startedAt, restart_count: e.restartCount || 0,
      is_playlist: !!(e.cfg.video_urls?.length),
    })), max: MAX_STREAMS },
  };
}

async function notifyStreamStopped(slot_id, reason) {
  const cbUrl = process.env.CALLBACK_URL;
  const vpsKey = process.env.VPS_KEY || API_KEY;
  if (!cbUrl) return;
  try {
    await fetch(cbUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-vps-key": vpsKey },
      body: JSON.stringify({ slot_id, reason }),
      signal: AbortSignal.timeout(8000),
    });
  } catch (_e) {}
}

function buildRtmpUrl(platform, streamKey) {
  switch (platform) {
    case "youtube":  return `rtmp://a.rtmp.youtube.com/live2/${streamKey}`;
    case "facebook": return `rtmps://live-api-s.facebook.com:443/rtmp/${streamKey}`;
    default:         return streamKey.startsWith("rtmp") ? streamKey : `rtmp://a.rtmp.youtube.com/live2/${streamKey}`;
  }
}

// ─── Smart FFmpeg args ────────────────────────────────────────────────────────
// First attempt: copy video (0% CPU if H.264), always re-encode audio to AAC
// On restart after failure: full transcode with libx264 veryfast
function buildFfmpegArgs(cfg, videoInputs, useTranscode = false) {
  const rtmpUrl = buildRtmpUrl(cfg.platform, cfg.stream_key);
  const args = [];

  if (videoInputs.length > 1) {
    const listFile = `/tmp/alp_playlist_${cfg.slot_id}.txt`;
    fs.writeFileSync(listFile, videoInputs.map(u => `file '${u}'`).join("\n"));
    args.push(
      "-re",
      "-stream_loop", cfg.loop ? "-1" : "0",
      "-f", "concat",
      "-safe", "0",
      "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
      "-i", listFile,
    );
  } else {
    args.push(
      "-re",
      "-stream_loop", cfg.loop ? "-1" : "0",
      "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
      "-i", videoInputs[0],
    );
  }

  if (useTranscode) {
    // Full transcode — works with any codec (H.265, VP9, etc.)
    args.push(
      "-c:v", "libx264",
      "-preset", "veryfast",
      "-tune", "zerolatency",
      "-x264-params", "keyint=60:min-keyint=60",
      "-b:v", `${cfg.bitrate || 4500}k`,
      "-maxrate", `${cfg.bitrate || 4500}k`,
      "-bufsize", `${(cfg.bitrate || 4500) * 2}k`,
      "-c:a", "aac",
      "-b:a", "128k",
      "-ar", "44100",
      "-f", "flv",
      "-flvflags", "no_duration_filesize",
      rtmpUrl
    );
  } else {
    // Smart copy — copy video codec (0% CPU if H.264), always AAC audio (~1% CPU)
    args.push(
      "-c:v", "copy",
      "-c:a", "aac",
      "-b:a", "128k",
      "-ar", "44100",
      "-f", "flv",
      "-flvflags", "no_duration_filesize",
      rtmpUrl
    );
  }

  return args;
}

function cleanupEntry(slotId) {
  const e = streams.get(slotId);
  if (e?.stopTimer) clearTimeout(e.stopTimer);
  const pf = `/tmp/alp_playlist_${slotId}.txt`;
  try { if (fs.existsSync(pf)) fs.unlinkSync(pf); } catch {}
  streams.delete(slotId);
}

function startFfmpeg(slotId, cfg, restartCount = 0) {
  const videoInputs = cfg.video_urls?.length ? cfg.video_urls : [cfg.video_url];

  // After 1 failed attempt with copy mode, switch to full transcode
  const useTranscode = restartCount >= 1;
  const args = buildFfmpegArgs(cfg, videoInputs, useTranscode);

  const proc = spawn("ffmpeg", args, { stdio: ["ignore", "pipe", "pipe"] });
  const entry = streams.get(slotId);
  if (!entry) return;

  entry.proc = proc;
  entry.status = "live";
  entry.startedAt = new Date().toISOString();
  entry.restartCount = restartCount;
  entry.useTranscode = useTranscode;

  if (cfg.callback_url) process.env.CALLBACK_URL = cfg.callback_url;
  if (cfg.vps_key)      process.env.VPS_KEY = cfg.vps_key;

  if (cfg.stop_after_min && Number(cfg.stop_after_min) > 0) {
    entry.stopTimer = setTimeout(() => {
      stopStream(slotId);
      notifyStreamStopped(slotId, "stop_after_min");
    }, cfg.stop_after_min * 60 * 1000);
  }

  const mode = useTranscode ? "TRANSCODE" : "COPY+AAC";
  console.log(`[${slotId}] ffmpeg started — mode: ${mode}, attempt: ${restartCount + 1}`);

  // Log ALL stderr — helps debug YouTube rejections
  let stderrBuf = "";
  proc.stderr.on("data", chunk => {
    const line = chunk.toString();
    stderrBuf += line;
    // Log errors and YouTube/RTMP related lines
    if (
      line.toLowerCase().includes("error") ||
      line.toLowerCase().includes("failed") ||
      line.toLowerCase().includes("refused") ||
      line.toLowerCase().includes("rtmp") ||
      line.toLowerCase().includes("codec") ||
      line.toLowerCase().includes("invalid") ||
      line.toLowerCase().includes("broken pipe")
    ) {
      console.error(`[${slotId}] ffmpeg: ${line.trim()}`);
    }
  });

  proc.on("exit", (code, signal) => {
    const manual = entry?.status === "stopped";
    if (!manual) {
      // First failure in copy mode → switch to transcode automatically
      if (code !== 0 && restartCount === 0 && !useTranscode) {
        console.log(`[${slotId}] copy mode failed (code ${code}), retrying with full transcode...`);
        if (streams.has(slotId)) streams.get(slotId).status = "restarting";
        setTimeout(() => {
          if (streams.has(slotId) && streams.get(slotId).status === "restarting")
            startFfmpeg(slotId, cfg, 1);
        }, 3000);
      } else if (code !== 0 && restartCount < 10) {
        const delay = Math.min(5000 * Math.pow(1.5, restartCount - 1), 60000);
        console.log(`[${slotId}] restarting in ${Math.round(delay/1000)}s (attempt ${restartCount + 1})`);
        if (streams.has(slotId)) streams.get(slotId).status = "restarting";
        setTimeout(() => {
          if (streams.has(slotId) && streams.get(slotId).status === "restarting")
            startFfmpeg(slotId, cfg, restartCount + 1);
        }, delay);
      } else {
        cleanupEntry(slotId);
        if (code !== 0) notifyStreamStopped(slotId, "unexpected_exit");
      }
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

function auth(req, res, next) {
  const k = req.headers["x-api-key"] || req.query.key || "";
  if (!safeEqual(k, API_KEY)) return res.status(401).json({ error: "Unauthorized" });
  next();
}

function pinOk(pin) { return pin && safeEqual(String(pin), String(CONSOLE_PIN)); }
function safePath(p) { return path.resolve(p || "/home/ubuntu"); }

app.get("/health", (_req, res) => res.json({ ok: true, streams: streams.size, max: MAX_STREAMS }));
app.get("/stats", (_req, res) => res.json(getStats()));

app.get("/stream/status", auth, (_req, res) => {
  const list = [...streams.values()].map(e => ({
    slot_id:       e.cfg.slot_id,
    platform:      e.cfg.platform || "unknown",
    status:        e.status || "live",
    started_at:    e.startedAt,
    restart_count: e.restartCount || 0,
    is_playlist:   !!(e.cfg.video_urls?.length),
    mode:          e.useTranscode ? "transcode" : "copy",
  }));
  res.json({ streams: list, count: list.length, max: MAX_STREAMS });
});

app.post("/stream/start", auth, (req, res) => {
  const { slot_id, video_url, video_urls, platform, stream_key,
    bitrate = 4500, resolution = "1080p", fps = 30,
    loop = true, stop_after_min = 0, callback_url, vps_key } = req.body || {};

  if (!slot_id)                  return res.status(400).json({ error: "slot_id required" });
  if (!platform || !stream_key)  return res.status(400).json({ error: "platform and stream_key required" });
  const isPlaylist = Array.isArray(video_urls) && video_urls.length > 0;
  if (!isPlaylist && !video_url) return res.status(400).json({ error: "video_url or video_urls required" });
  if (streams.has(slot_id))      return res.status(409).json({ error: "Stream already running" });
  if (streams.size >= MAX_STREAMS) return res.status(429).json({ error: `Max streams reached (${MAX_STREAMS})` });

  const cfg = { slot_id, video_url, video_urls, platform, stream_key,
    bitrate, resolution, fps, loop, stop_after_min, callback_url, vps_key };
  streams.set(slot_id, { cfg, proc: null, status: "starting", startedAt: null, restartCount: 0, stopTimer: null, useTranscode: false });
  startFfmpeg(slot_id, cfg);

  console.log(`[${slot_id}] stream queued (${isPlaylist ? `playlist ${video_urls.length} videos` : "single video"})`);
  res.json({ ok: true, slot_id, mode: "smart-copy" });
});

app.post("/stream/stop", auth, (req, res) => {
  const { slot_id } = req.body || {};
  if (!slot_id) return res.status(400).json({ error: "slot_id required" });
  if (!stopStream(slot_id)) return res.status(404).json({ error: "No active stream" });
  console.log(`[${slot_id}] stopped via API`);
  res.json({ ok: true, slot_id });
});

// ─── Terminal ─────────────────────────────────────────────────────────────────
app.post("/terminal/auth", auth, (req, res) => {
  const { pin } = req.body || {};
  res.json({ ok: pinOk(pin) });
});

app.post("/terminal/run", auth, (req, res) => {
  const { cmd, pin } = req.body || {};
  if (!pinOk(pin)) return res.status(401).json({ error: "Invalid PIN" });
  if (!cmd) return res.status(400).json({ error: "cmd required" });
  try {
    const stdout = execSync(cmd, { timeout: 25000, maxBuffer: 2 * 1024 * 1024 }).toString();
    res.json({ stdout, stderr: "" });
  } catch (e) {
    res.json({
      stdout: e.stdout ? e.stdout.toString() : "",
      stderr: e.stderr ? e.stderr.toString() : e.message,
    });
  }
});

// ─── File Manager ─────────────────────────────────────────────────────────────
app.post("/fm/list", auth, (req, res) => {
  const { pin, path: p } = req.body || {};
  if (!pinOk(pin)) return res.status(401).json({ error: "Invalid PIN" });
  const dir = safePath(p);
  try {
    const items = fs.readdirSync(dir, { withFileTypes: true }).map(d => {
      const full = path.join(dir, d.name);
      let size = 0;
      try { if (d.isFile()) size = fs.statSync(full).size; } catch {}
      return { name: d.name, path: full, dir: d.isDirectory(), size };
    }).sort((a, b) => {
      if (a.dir && !b.dir) return -1;
      if (!a.dir && b.dir) return 1;
      return a.name.localeCompare(b.name);
    });
    res.json({ items, cwd: dir });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/fm/read", auth, (req, res) => {
  const { pin, path: p } = req.body || {};
  if (!pinOk(pin)) return res.status(401).json({ error: "Invalid PIN" });
  const target = safePath(p);
  try {
    const stat = fs.statSync(target);
    if (stat.size > 512 * 1024) return res.status(400).json({ error: "File too large to edit (max 512KB)" });
    const content = fs.readFileSync(target, "utf8");
    res.json({ content });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/fm/write", auth, (req, res) => {
  const { pin, path: p, content } = req.body || {};
  if (!pinOk(pin)) return res.status(401).json({ error: "Invalid PIN" });
  if (content === undefined) return res.status(400).json({ error: "content required" });
  const target = safePath(p);
  try {
    fs.writeFileSync(target, content, "utf8");
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/fm/mkdir", auth, (req, res) => {
  const { pin, path: p } = req.body || {};
  if (!pinOk(pin)) return res.status(401).json({ error: "Invalid PIN" });
  try { fs.mkdirSync(safePath(p), { recursive: true }); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/fm/delete", auth, (req, res) => {
  const { pin, path: p } = req.body || {};
  if (!pinOk(pin)) return res.status(401).json({ error: "Invalid PIN" });
  try {
    const target = safePath(p);
    const stat = fs.statSync(target);
    if (stat.isDirectory()) fs.rmSync(target, { recursive: true, force: true });
    else fs.unlinkSync(target);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

const upload = multer({ dest: "/tmp/fm-uploads/" });
app.post("/fm/upload", auth, upload.single("file"), (req, res) => {
  const pin  = req.body?.pin;
  const dest = req.body?.path;
  if (!pinOk(pin)) {
    try { fs.unlinkSync(req.file.path); } catch {}
    return res.status(401).json({ error: "Invalid PIN" });
  }
  try {
    const target = path.join(safePath(dest), req.file.originalname);
    fs.renameSync(req.file.path, target);
    res.json({ ok: true, file: target });
  } catch (e) {
    try { fs.unlinkSync(req.file.path); } catch {}
    res.status(500).json({ error: e.message });
  }
});

app.get("/fm/download", auth, (req, res) => {
  const { pin, path: p } = req.query;
  if (!pinOk(pin)) return res.status(401).send("Unauthorized");
  try { res.download(safePath(String(p))); }
  catch (e) { res.status(500).send(e.message); }
});

// ─── WebSocket ────────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server, path: "/ws/logs" });
wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://localhost`);
  if (!safeEqual(url.searchParams.get("key") || "", API_KEY)) { ws.close(4001, "Unauthorized"); return; }
  ws.send(JSON.stringify({ type: "connected", time: new Date().toISOString() }));
  const tick = setInterval(() => {
    const list = [...streams.values()].map(e => ({ slot_id: e.cfg.slot_id, status: e.status, started_at: e.startedAt }));
    try { ws.send(JSON.stringify({ type: "status", streams: list })); } catch {}
  }, 2000);
  ws.on("close", () => clearInterval(tick));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`\n🚀  AutoLive Pro VPS Agent v3 — port ${PORT}  (max: ${MAX_STREAMS} streams, SMART-COPY MODE)\n`);
});
