require("dotenv").config();
const express  = require("express");
const http     = require("http");
const { WebSocketServer } = require("ws");
const { spawn, execFile, execFileSync } = require("child_process");
const fs       = require("fs");
const path     = require("path");
const os       = require("os");
const multer   = require("multer");
const crypto   = require("crypto");

const app    = express();
const server = http.createServer(app);

const PORT         = Number(process.env.PORT || 5001);
const API_KEY      = process.env.API_KEY || "changeme";
const CONSOLE_PIN  = String(process.env.CONSOLE_PIN || "1234");
const MAX_STREAMS  = Number(process.env.MAX_STREAMS || 15);
const CACHE_DIR    = process.env.CACHE_DIR || "/var/alp/cache";
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_HOURS || 24) * 60 * 60 * 1000;

try { fs.mkdirSync(CACHE_DIR, { recursive: true }); } catch {}

app.use(express.json({ limit: "10mb" }));

function safeEqual(a, b) {
  try {
    const ba = Buffer.from(String(a).padEnd(64));
    const bb = Buffer.from(String(b).padEnd(64));
    return crypto.timingSafeEqual(ba, bb) && a.length === b.length;
  } catch { return false; }
}

const streams = new Map();

// ─── Detect once at startup whether nice/ionice exist on this box ──────────
let HAS_NICE = false, HAS_IONICE = false;
try { execFileSync("which", ["nice"]);   HAS_NICE = true;   } catch {}
try { execFileSync("which", ["ionice"]); HAS_IONICE = true; } catch {}

function liveStreamCount() {
  return [...streams.values()].filter(e => e.status === "live").length;
}

// Wrap an ffmpeg invocation with OS-level CPU/IO priority so heavy one-off
// transcode jobs never starve already-running live streams on the same box.
// `background=true` => this job should yield to live streams (idle priority,
// single thread). `background=false` => box is otherwise idle, run faster.
function spawnPrioritized(cmd, args, background) {
  const prefix = [];
  if (HAS_NICE)   prefix.push("nice", "-n", background ? "15" : "5");
  if (HAS_IONICE) prefix.push("ionice", "-c", background ? "3" : "2");
  if (prefix.length) return spawn(prefix[0], [...prefix.slice(1), cmd, ...args], { stdio: ["ignore", "pipe", "pipe"] });
  return spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"] });
}

// ─── Network stats (cheap, in-process, no subprocess) ─────────────────────────
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

// ─── Disk stats — refreshed in the background on a timer, never blocking ──────
// (old code ran `df -BG` synchronously on every /stats request, which could
//  stall the event loop for a few ms right while ffmpeg pipes/timers needed
//  to be serviced — under load that shows up as CPU/stream jitter)
let _diskStats = { pct: 0, used_gb: 0, total_gb: 50 };
function refreshDiskStatsAsync() {
  execFile("df", ["-BG", "/"], { timeout: 5000 }, (err, stdout) => {
    if (err || !stdout) return;
    try {
      const line = stdout.trim().split("\n").pop();
      const df = line.trim().split(/\s+/);
      const total = parseInt(df[1]) || 50;
      const used  = parseInt(df[2]) || 0;
      _diskStats = { pct: total ? Math.round((used / total) * 100) : 0, used_gb: used, total_gb: total };
    } catch {}
  });
}
refreshDiskStatsAsync();
setInterval(refreshDiskStatsAsync, 30000);

function getStats() {
  let cpuPct = 0, cores = os.cpus().length;
  try {
    const load = os.loadavg()[0];
    cpuPct = Math.min(100, Math.round((load / cores) * 100));
  } catch {}

  const totalMem = os.totalmem(), freeMem = os.freemem();
  const usedMem  = totalMem - freeMem;
  const net = getNetStats();

  return {
    cpu:          { pct: cpuPct, cores },
    ram:          { pct: Math.round((usedMem/totalMem)*100), used_mb: Math.round(usedMem/1024/1024), total_mb: Math.round(totalMem/1024/1024) },
    disk:         { pct: _diskStats.pct, used_gb: _diskStats.used_gb, total_gb: _diskStats.total_gb },
    network:      net,
    uptime:       Math.floor(os.uptime()),
    streams:      { list: [...streams.values()].map(e => ({
      slot_id: e.cfg.slot_id, platform: e.cfg.platform, status: e.status,
      started_at: e.startedAt, restart_count: e.restartCount || 0,
      is_playlist: !!(e.cfg.video_urls?.length), mode: e.useTranscode ? "transcode" : "copy",
    })), max: MAX_STREAMS },
  };
}

async function notifyUrl(url, vpsKey, payload) {
  if (!url) return;
  try {
    await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-vps-key": vpsKey || API_KEY },
      body: JSON.stringify(payload),
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

// ─── Local video caching (with 24h TTL eviction) ───────────────────────────
// Old code pointed ffmpeg straight at the remote CDN URL with -stream_loop -1.
// On every loop iteration ffmpeg had to re-open the HTTP connection to R2 —
// that re-open (TLS handshake + HTTP GET) is what showed up as CPU/network
// spikes every loop, independent of copy vs transcode mode.
// Fix: download once to local disk, then loop the LOCAL file. Cache is keyed
// by URL hash so repeated starts of the same video reuse the cached file.
// Every time a cached file is reused, its mtime is bumped — a periodic sweep
// then deletes anything untouched for CACHE_TTL_HOURS (default 24h), freeing
// VPS disk while the original always stays safe on R2 until the user deletes it.
function cacheKeyForUrl(url) {
  return crypto.createHash("sha1").update(url).digest("hex");
}

function touchCacheFile(p) {
  try { const now = new Date(); fs.utimesSync(p, now, now); } catch {}
}

function downloadToCache(url) {
  return new Promise((resolve, reject) => {
    const key = cacheKeyForUrl(url);
    const dest = path.join(CACHE_DIR, `${key}.mp4`);
    const tmp  = `${dest}.part`;

    if (fs.existsSync(dest) && fs.statSync(dest).size > 0) {
      touchCacheFile(dest);
      return resolve(dest);
    }

    execFile("curl", ["-fsSL", "--retry", "3", "-o", tmp, url], { timeout: 10 * 60 * 1000 }, (err) => {
      if (err) {
        try { fs.unlinkSync(tmp); } catch {}
        return reject(err);
      }
      try {
        fs.renameSync(tmp, dest);
        resolve(dest);
      } catch (e) { reject(e); }
    });
  });
}

async function resolveLocalInputs(videoInputs) {
  const local = [];
  for (const url of videoInputs) {
    try {
      local.push(await downloadToCache(url));
    } catch (e) {
      console.error(`[cache] failed to cache ${url}: ${e.message} — falling back to direct URL`);
      local.push(url);
    }
  }
  return local;
}

function sweepStaleCache() {
  let files;
  try { files = fs.readdirSync(CACHE_DIR); } catch { return; }
  const now = Date.now();
  for (const f of files) {
    if (!f.endsWith(".mp4")) continue;
    const full = path.join(CACHE_DIR, f);
    try {
      const st = fs.statSync(full);
      if (now - st.mtimeMs > CACHE_TTL_MS) {
        fs.unlinkSync(full);
        console.log(`[cache] evicted stale file (unused > ${CACHE_TTL_MS / 3600000}h): ${f}`);
      }
    } catch {}
  }
}
setInterval(sweepStaleCache, 60 * 60 * 1000); // hourly sweep

// ─── Codec detection (async, off the event loop) ───────────────────────────
// Old code only learned the codec was incompatible AFTER ffmpeg crashed in
// copy mode, then blindly escalated every single failure (including
// transient RTMP/network blips) straight to a full libx264 transcode —
// that's the main reason CPU jumped from ~2% to 80-100%. We now ffprobe the
// (now-local) file up front, once, and only pick transcode mode when the
// codec genuinely isn't H.264.
function detectVideoCodec(filePath) {
  return new Promise((resolve) => {
    execFile("ffprobe", [
      "-v", "quiet", "-select_streams", "v:0",
      "-show_entries", "stream=codec_name",
      "-of", "default=noprint_wrappers=1:nokey=1",
      filePath,
    ], { timeout: 10000 }, (err, stdout) => {
      if (err) return resolve("unknown");
      resolve((stdout || "").toString().trim() || "unknown");
    });
  });
}

// ─── Smart FFmpeg args ────────────────────────────────────────────────────────
// `threadCap` keeps any single ffmpeg process from monopolizing every core on
// the box — without it, one stream that needs a full transcode can spike to
// 100% CPU and starve every other stream sharing the VPS.
function buildFfmpegArgs(cfg, videoInputs, useTranscode, threadCap) {
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
    // ultrafast preset + hard resolution cap + thread cap keeps CPU usage
    // bounded even on a small VPS shared by several other live streams.
    const bitrate = Math.min(Number(cfg.bitrate) || 4500, 4500);
    const scaleFilter = "scale='min(1920,iw)':'min(1080,ih)':force_original_aspect_ratio=decrease";
    args.push(
      "-vf", scaleFilter,
      "-c:v", "libx264",
      "-preset", "ultrafast",
      "-threads", String(threadCap),
      "-tune", "zerolatency",
      "-x264-params", "keyint=60:min-keyint=60",
      "-b:v", `${bitrate}k`,
      "-maxrate", `${bitrate}k`,
      "-bufsize", `${bitrate * 2}k`,
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

async function startFfmpeg(slotId, cfg, restartCount = 0, opts = {}) {
  const entry = streams.get(slotId);
  if (!entry) return;

  const rawInputs = cfg.video_urls?.length ? cfg.video_urls : [cfg.video_url];

  // Resolve to local cached files (download once, reused on every restart/loop,
  // and reused by the upload-time /process job if it already cached this URL).
  let videoInputs;
  try {
    videoInputs = await resolveLocalInputs(rawInputs);
  } catch (e) {
    videoInputs = rawInputs; // fall back to remote URL if caching totally fails
  }

  if (!streams.has(slotId)) return; // stream may have been stopped while we were downloading

  // Decide mode: prefer the ffprobe-confirmed codec on the first attempt;
  // only escalate to transcode automatically once copy mode has genuinely
  // failed (handled in the exit handler below).
  let useTranscode = opts.forceTranscode === true || restartCount >= 2;
  if (restartCount === 0 && !useTranscode) {
    const codec = await detectVideoCodec(videoInputs[0]);
    if (codec !== "h264" && codec !== "unknown") {
      console.log(`[${slotId}] ffprobe detected codec=${codec} (not h264) — starting in transcode mode`);
      useTranscode = true;
    }
  }

  // Spread CPU across however many OTHER streams are already live, so a
  // transcode-mode stream never grabs every core on the box.
  const cores = os.cpus().length;
  const otherLive = Math.max(0, liveStreamCount());
  const threadCap = Math.max(1, Math.floor(cores / (otherLive + 1)));

  const args = buildFfmpegArgs(cfg, videoInputs, useTranscode, threadCap);
  const proc = spawnPrioritized("ffmpeg", args, /* background */ false); // live streams always run at normal priority
  if (!streams.has(slotId)) { try { proc.kill("SIGKILL"); } catch {} return; }

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
      notifyUrl(process.env.CALLBACK_URL, process.env.VPS_KEY, { slot_id: slotId, reason: "stop_after_min" });
    }, cfg.stop_after_min * 60 * 1000);
  }

  const mode = useTranscode ? "TRANSCODE" : "COPY+AAC";
  console.log(`[${slotId}] ffmpeg started — mode: ${mode}, attempt: ${restartCount + 1}, threads: ${threadCap}`);

  // Log ALL stderr — helps debug YouTube rejections
  proc.stderr.on("data", chunk => {
    const line = chunk.toString();
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
      // First failure in copy mode → switch to transcode automatically.
      // (Still only one automatic escalation — repeated non-zero exits after
      // that are treated as transient and just retried in the SAME mode with
      // backoff, instead of being assumed to be a codec problem.)
      if (code !== 0 && restartCount === 0 && !useTranscode) {
        console.log(`[${slotId}] copy mode failed (code ${code}), retrying with full transcode...`);
        if (streams.has(slotId)) streams.get(slotId).status = "restarting";
        setTimeout(() => {
          if (streams.has(slotId) && streams.get(slotId).status === "restarting")
            startFfmpeg(slotId, cfg, 1, { forceTranscode: true });
        }, 3000);
      } else if (code !== 0 && restartCount < 10) {
        const delay = Math.min(5000 * Math.pow(1.5, restartCount - 1), 60000);
        console.log(`[${slotId}] restarting in ${Math.round(delay/1000)}s (attempt ${restartCount + 1})`);
        if (streams.has(slotId)) streams.get(slotId).status = "restarting";
        setTimeout(() => {
          if (streams.has(slotId) && streams.get(slotId).status === "restarting")
            startFfmpeg(slotId, cfg, restartCount + 1, { forceTranscode: useTranscode });
        }, delay);
      } else {
        cleanupEntry(slotId);
        if (code !== 0) notifyUrl(process.env.CALLBACK_URL, process.env.VPS_KEY, { slot_id: slotId, reason: "unexpected_exit" });
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

// ─── Upload-time processing ─────────────────────────────────────────────────
// Lets the data server hand a freshly-uploaded video to "whichever VPS is
// free" right after upload, instead of only ever finding out the codec is
// wrong at first-stream-start time. Runs at idle CPU/IO priority and with a
// single thread whenever this VPS already has live streams running, so it
// never competes with them — and runs at normal priority/full threads when
// the box is otherwise idle.
const processingJobs = new Map(); // video_id -> { startedAt }

function activeProcessingCount() { return processingJobs.size; }

app.post("/process", auth, (req, res) => {
  const { video_id, source_url, callback_url, vps_key } = req.body || {};
  if (!video_id || !source_url) return res.status(400).json({ error: "video_id and source_url required" });
  if (processingJobs.has(video_id)) return res.json({ ok: true, queued: true, already_running: true });

  res.json({ ok: true, queued: true });
  processingJobs.set(video_id, { startedAt: Date.now() });
  processVideo(video_id, source_url, callback_url, vps_key)
    .catch(e => console.error(`[process] ${video_id} failed:`, e.message))
    .finally(() => processingJobs.delete(video_id));
});

async function processVideo(videoId, sourceUrl, callbackUrl, vpsKey) {
  let localPath;
  try {
    localPath = await downloadToCache(sourceUrl);
  } catch (e) {
    return notifyUrl(callbackUrl, vpsKey, { video_id: videoId, ok: false, error: "download_failed: " + e.message });
  }

  const codec = await detectVideoCodec(localPath);
  if (codec === "h264") {
    console.log(`[process] ${videoId} already h264 — cached, no transcode needed`);
    return notifyUrl(callbackUrl, vpsKey, { video_id: videoId, ok: true, codec, transcoded: false });
  }

  // Box is "busy" if any live stream is currently running here — in that
  // case the transcode runs at idle nice/ionice priority on a single thread
  // so the live stream(s) keep all the CPU they need.
  const busy = liveStreamCount() > 0;
  const threads = busy ? 1 : Math.max(1, os.cpus().length - 1);
  const outPath = `${localPath}.tmp.mp4`;

  console.log(`[process] ${videoId} codec=${codec} → transcoding to h264 (${busy ? "background/idle priority" : "normal priority"}, threads=${threads})`);

  try {
    await new Promise((resolve, reject) => {
      const proc = spawnPrioritized("ffmpeg", [
        "-i", localPath,
        "-c:v", "libx264", "-preset", busy ? "ultrafast" : "veryfast",
        "-threads", String(threads),
        "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
        "-movflags", "+faststart",
        "-y", outPath,
      ], busy);
      let stderr = "";
      proc.stderr.on("data", d => { stderr += d.toString(); });
      proc.on("exit", code => code === 0 ? resolve() : reject(new Error(`ffmpeg exited ${code}: ${stderr.slice(-300)}`)));
      proc.on("error", reject);
    });
    fs.renameSync(outPath, localPath); // cached file is now normalized H.264 — future streams get instant copy-mode
    touchCacheFile(localPath);
    console.log(`[process] ${videoId} transcoded to h264 successfully`);
    notifyUrl(callbackUrl, vpsKey, { video_id: videoId, ok: true, codec: "h264", transcoded: true });
  } catch (e) {
    try { fs.unlinkSync(outPath); } catch {}
    console.error(`[process] ${videoId} transcode failed: ${e.message}`);
    notifyUrl(callbackUrl, vpsKey, { video_id: videoId, ok: false, error: e.message });
  }
}

function auth(req, res, next) {
  const k = req.headers["x-api-key"] || req.query.key || "";
  if (!safeEqual(k, API_KEY)) return res.status(401).json({ error: "Unauthorized" });
  next();
}

function pinOk(pin) { return pin && safeEqual(String(pin), String(CONSOLE_PIN)); }
function safePath(p) { return path.resolve(p || "/home/ubuntu"); }

app.get("/health", (_req, res) => res.json({ ok: true, streams: streams.size, max: MAX_STREAMS, processing: activeProcessingCount() }));
app.get("/stats", (_req, res) => res.json({ ...getStats(), processing_jobs: activeProcessingCount() }));

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
    const stdout = execFileSync("/bin/sh", ["-c", cmd], { timeout: 25000, maxBuffer: 2 * 1024 * 1024 }).toString();
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

// ─── Cache management ───────────────────────────────────────────────────────
app.get("/cache/stats", auth, (_req, res) => {
  try {
    const files = fs.readdirSync(CACHE_DIR);
    let totalBytes = 0;
    for (const f of files) { try { totalBytes += fs.statSync(path.join(CACHE_DIR, f)).size; } catch {} }
    res.json({ dir: CACHE_DIR, files: files.length, total_mb: Math.round(totalBytes / 1024 / 1024), ttl_hours: CACHE_TTL_MS / 3600000 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/cache/clear", auth, (_req, res) => {
  try {
    for (const f of fs.readdirSync(CACHE_DIR)) {
      try { fs.unlinkSync(path.join(CACHE_DIR, f)); } catch {}
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
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
  console.log(`\n🚀  AutoLive Pro VPS Agent v4 — port ${PORT}  (max: ${MAX_STREAMS} streams, SMART-COPY + LOCAL-CACHE + LOAD-AWARE PROCESSING)\n`);
});
