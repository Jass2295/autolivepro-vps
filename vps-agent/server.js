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
const API_KEY      = String(process.env.API_KEY || "");
const CONSOLE_PIN  = String(process.env.CONSOLE_PIN || "");
const MAX_STREAMS  = Number(process.env.MAX_STREAMS || 15);
// Default cache dir lives under the agent's own home directory, not /var —
// /var is root-owned on most VPS images, so a non-root pm2 user (e.g. "ubuntu")
// would silently fail to create /var/alp/cache and every cache write would
// then fail with "curl: (23) Failure writing output to destination".
const CACHE_DIR    = process.env.CACHE_DIR || path.join(os.homedir(), ".alp-cache");
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_HOURS || 24) * 60 * 60 * 1000;
const ADMIN_SCOPE  = path.resolve(process.env.ADMIN_SCOPE || path.join(os.homedir(), ".autolive-admin-scope"));
const STATE_FILE   = path.resolve(process.env.STATE_FILE || path.join(os.homedir(), ".autolivepro-streams.json"));

if (API_KEY.length < 32) throw new Error("API_KEY is required and must be at least 32 characters");
if (CONSOLE_PIN.length < 8) throw new Error("CONSOLE_PIN is required and must be at least 8 characters");

try {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
  fs.accessSync(CACHE_DIR, fs.constants.W_OK);
  fs.mkdirSync(ADMIN_SCOPE, { recursive: true, mode: 0o700 });
  fs.chmodSync(ADMIN_SCOPE, 0o700);
  fs.mkdirSync(path.dirname(STATE_FILE), { recursive: true });
} catch (e) {
  console.error(`[cache] FATAL: cannot create/write CACHE_DIR "${CACHE_DIR}": ${e.message}`);
  console.error(`[cache] Set CACHE_DIR env var to a writable path (e.g. ${path.join(os.homedir(), ".alp-cache")}) and restart.`);
}

app.use(express.json({ limit: "10mb" }));

function safeEqual(a, b) {
  try {
    const ba = Buffer.from(String(a));
    const bb = Buffer.from(String(b));
    return ba.length === bb.length && crypto.timingSafeEqual(ba, bb);
  } catch { return false; }
}

const streams = new Map();
const downloadInFlight = new Map();
const activeDownloads = new Map();
let isPurging = false;
const STATE_KEY = crypto.createHash("sha256").update(API_KEY).digest();

function persistStreams() {
  try {
    const snapshot = [...streams.values()]
      .filter((entry) => entry.status !== "stopped")
      .map((entry) => ({ cfg: entry.cfg, status: entry.status }));
    const tmp = `${STATE_FILE}.tmp`;
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv("aes-256-gcm", STATE_KEY, iv);
    const encrypted = Buffer.concat([cipher.update(JSON.stringify(snapshot), "utf8"), cipher.final()]);
    const payload = {
      version: 1,
      iv: iv.toString("base64"),
      tag: cipher.getAuthTag().toString("base64"),
      data: encrypted.toString("base64"),
    };
    fs.writeFileSync(tmp, JSON.stringify(payload), { mode: 0o600 });
    fs.chmodSync(tmp, 0o600);
    fs.renameSync(tmp, STATE_FILE);
  } catch (error) {
    console.error(`[state] could not persist streams: ${error.message}`);
  }
}

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
  if (isPurging) return Promise.reject(new Error("AutoLive cleanup is in progress"));
  if (downloadInFlight.has(url)) return downloadInFlight.get(url);
  const job = new Promise((resolve, reject) => {
    const key = cacheKeyForUrl(url);
    const dest = path.join(CACHE_DIR, `${key}.mp4`);
    const tmp  = `${dest}.part`;

    if (fs.existsSync(dest) && fs.statSync(dest).size > 0) {
      touchCacheFile(dest);
      return resolve(dest);
    }

    let child;
    child = execFile("curl", ["-fsSL", "--retry", "3", "-o", tmp, url], { timeout: 10 * 60 * 1000 }, (err) => {
      if (activeDownloads.get(url) === child) activeDownloads.delete(url);
      if (err) {
        try { fs.unlinkSync(tmp); } catch {}
        return reject(err);
      }
      try {
        fs.renameSync(tmp, dest);
        resolve(dest);
      } catch (e) { reject(e); }
    });
    activeDownloads.set(url, child);
  });
  downloadInFlight.set(url, job);
  job.finally(() => downloadInFlight.delete(url)).catch(() => {});
  return job;
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
  return probeVideo(filePath).then(i => i.codec);
}

// Full probe: codec + pixel format + profile + size + fps + keyframe gap.
// Copy mode only survives on YouTube/FB if the file is "live friendly", so
// codec_name === "h264" alone is NOT enough (10-bit / 4:2:2 / huge GOP /
// 4K / VFR phone videos are all h264 but break or stutter in copy mode).
function probeVideo(filePath) {
  return new Promise((resolve) => {
    execFile("ffprobe", [
      "-v", "quiet", "-select_streams", "v:0",
      "-show_entries", "stream=codec_name,profile,pix_fmt,width,height,avg_frame_rate,r_frame_rate",
      "-of", "json", filePath,
    ], { timeout: 15000 }, (err, stdout) => {
      if (err) return resolve({ codec: "unknown" });
      let st = {};
      try { st = (JSON.parse(stdout).streams || [])[0] || {}; } catch {}
      const info = {
        codec: (st.codec_name || "unknown").toLowerCase(),
        profile: st.profile || "", pix_fmt: st.pix_fmt || "",
        width: Number(st.width) || 0, height: Number(st.height) || 0,
        fps: evalFps(st.avg_frame_rate) || evalFps(st.r_frame_rate) || 0,
        vfr: !!(st.avg_frame_rate && st.r_frame_rate && st.avg_frame_rate !== st.r_frame_rate),
        maxGop: 0,
      };
      // keyframe gap over the first 90s (cheap: packet headers only)
      execFile("ffprobe", [
        "-v", "quiet", "-select_streams", "v:0", "-read_intervals", "%90",
        "-show_entries", "packet=pts_time,flags", "-of", "csv=p=0", filePath,
      ], { timeout: 30000, maxBuffer: 64 * 1024 * 1024 }, (e2, out) => {
        if (!e2 && out) {
          let last = null, max = 0;
          for (const l of out.toString().split("\n")) {
            const [t, f] = l.split(",");
            if (f && f.includes("K") && t && t !== "N/A") {
              const ts = parseFloat(t);
              if (last !== null && ts - last > max) max = ts - last;
              last = ts;
            }
          }
          info.maxGop = max;
        }
        resolve(info);
      });
    });
  });
}
function evalFps(v) {
  if (!v || v === "0/0") return 0;
  const [a, b] = String(v).split("/").map(Number);
  return b ? a / b : a || 0;
}
// true => file must be normalized at upload time so live copy-mode is safe
function needsNormalize(i) {
  if (i.codec === "unknown") return false;           // can't tell; don't burn CPU
  if (i.codec !== "h264") return true;
  if (i.pix_fmt && i.pix_fmt !== "yuv420p") return true;           // 10-bit / 4:2:2 / 4:4:4
  if (/High 10|High 4:2:2|High 4:4:4|Hi10|Hi422|Hi444/i.test(i.profile)) return true;
  if (i.height > 1080 || i.width > 1920) return true;
  if (i.fps > 60.5) return true;
  if (i.vfr) return true;                            // variable fps => audio/video drift in copy
  if (i.maxGop > 4.5) return true;                   // YouTube wants a keyframe at least every 4s
  return false;
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
     fs.writeFileSync(listFile, videoInputs.map(u => `file '${String(u).replace(/'/g, "'\\''")}'`).join("\n"));
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
      "-fflags", "+genpts",
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
  persistStreams();
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

  if (streams.get(slotId) !== entry) return; // stream may have been stopped while we were downloading

  // Decide mode: prefer the ffprobe-confirmed codec on the first attempt;
  // only escalate to transcode automatically once copy mode has genuinely
  // failed (handled in the exit handler below).
  let useTranscode = opts.forceTranscode === true;
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
  if (streams.get(slotId) !== entry) { try { proc.kill("SIGKILL"); } catch {} return; }

  entry.proc = proc;
  entry.status = "live";
  entry.startedAt = new Date().toISOString();
  entry.restartCount = restartCount;
  entry.useTranscode = useTranscode;
  persistStreams();
  notifyUrl(cfg.callback_url, cfg.vps_key, {
    slot_id: slotId, status: "live", started_at: entry.startedAt,
  });

  if (cfg.stop_after_min && Number(cfg.stop_after_min) > 0) {
    entry.stopTimer = setTimeout(() => {
      stopStream(slotId);
      notifyUrl(cfg.callback_url, cfg.vps_key, { slot_id: slotId, reason: "stop_after_min" });
    }, cfg.stop_after_min * 60 * 1000);
  }

  const mode = useTranscode ? "TRANSCODE" : "COPY+AAC";
  console.log(`[${slotId}] ffmpeg started — mode: ${mode}, attempt: ${restartCount + 1}, threads: ${threadCap}`);

  let errTail = "";
  const runStart = Date.now();
  // Log ALL stderr — helps debug YouTube rejections
  proc.stderr.on("data", chunk => {
    const line = chunk.toString();
    errTail = (errTail + line).slice(-1500);
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
    if (manual) { cleanupEntry(slotId); return; }

    const ranMs = Date.now() - runStart;
    // A run that stayed up >2 min was healthy: forget old failures, so one
    // network blip a day never adds up to a permanent transcode.
    const effRestart = ranMs > 120000 ? 0 : restartCount;

    // Only a REAL media/codec problem justifies switching to transcode.
    // RTMP drops, broken pipe, connection reset, DNS etc. just retry in COPY.
    const codecProblem = /Invalid data found|Unsupported|not supported|Could not write header|codec.*(not|unknown)|Error initializing|Error while opening|unspecified pixel format|incorrect codec parameters|moov atom not found|no decoder/i.test(errTail);
    const netProblem = /Broken pipe|Connection (reset|refused|timed out)|Input\/output error|Server returned|Error writing trailer|Cannot open connection|Immediate exit requested/i.test(errTail);

    console.log(`[${slotId}] ffmpeg exited code=${code} ran=${Math.round(ranMs/1000)}s codecProblem=${codecProblem} netProblem=${netProblem}\n  last stderr: ${errTail.trim().split("\n").slice(-3).join(" | ")}`);

    if (code !== 0 && !useTranscode && codecProblem && !netProblem && ranMs < 120000) {
      console.log(`[${slotId}] copy failed because of codec/format — one-off transcode retry`);
      if (streams.get(slotId) === entry) streams.get(slotId).status = "restarting";
      persistStreams();
      setTimeout(() => {
        if (streams.get(slotId) === entry && streams.get(slotId).status === "restarting")
          startFfmpeg(slotId, cfg, 1, { forceTranscode: true });
      }, 3000);
    } else if ((code !== 0 || (cfg.loop && ranMs > 5000)) && effRestart < 10) {
      // stay in the SAME mode, always try copy again after a transient failure
      const delay = Math.min(3000 * Math.pow(1.5, effRestart), 60000);
      console.log(`[${slotId}] restarting in ${Math.round(delay/1000)}s in ${useTranscode ? "TRANSCODE" : "COPY"} mode (attempt ${effRestart + 2})`);
      if (streams.get(slotId) === entry) streams.get(slotId).status = "restarting";
      persistStreams();
      setTimeout(() => {
        if (streams.get(slotId) === entry && streams.get(slotId).status === "restarting")
          startFfmpeg(slotId, cfg, effRestart + 1, { forceTranscode: useTranscode });
      }, delay);
    } else {
      cleanupEntry(slotId);
      notifyUrl(cfg.callback_url, cfg.vps_key, {
        slot_id: slotId, reason: code === 0 ? "ffmpeg_exit" : "unexpected_exit",
      });
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
  if (isPurging) return res.status(503).json({ error: "AutoLive cleanup is in progress" });
  const { video_id, source_url, callback_url, vps_key } = req.body || {};
  if (!video_id || !source_url) return res.status(400).json({ error: "video_id and source_url required" });
  if (processingJobs.has(video_id)) return res.json({ ok: true, queued: true, already_running: true });

  res.json({ ok: true, queued: true });
  const job = { startedAt: Date.now(), cancelled: false, proc: null, done: null };
  processingJobs.set(video_id, job);
  job.done = processVideo(video_id, source_url, callback_url, vps_key, job)
    .catch(e => console.error(`[process] ${video_id} failed:`, e.message))
    .finally(() => processingJobs.delete(video_id));
});

async function processVideo(videoId, sourceUrl, callbackUrl, vpsKey, job) {
  let localPath;
  try {
    localPath = await downloadToCache(sourceUrl);
  } catch (e) {
    if (job.cancelled) return;
    return notifyUrl(callbackUrl, vpsKey, { video_id: videoId, ok: false, error: "download_failed: " + e.message });
  }
  if (job.cancelled) return;

  const info = await probeVideo(localPath);
  const codec = info.codec;
  if (job.cancelled) return;
  if (!needsNormalize(info)) {
    console.log(`[process] ${videoId} live-friendly (${codec} ${info.pix_fmt} ${info.width}x${info.height} @${info.fps.toFixed(1)} gop<=${info.maxGop.toFixed(1)}s) — copy mode OK`);
    if (job.cancelled) return;
    return notifyUrl(callbackUrl, vpsKey, { video_id: videoId, ok: true, codec, transcoded: false });
  }

  // Box is "busy" if any live stream is currently running here — in that
  // case the transcode runs at idle nice/ionice priority on a single thread
  // so the live stream(s) keep all the CPU they need.
  const busy = liveStreamCount() > 0;
  const threads = busy ? 1 : Math.max(1, os.cpus().length - 1);
  const outPath = `${localPath}.tmp.mp4`;

  console.log(`[process] ${videoId} NOT live-friendly (${codec} ${info.pix_fmt} ${info.width}x${info.height} @${info.fps.toFixed(1)} gop<=${info.maxGop.toFixed(1)}s) → normalizing to h264 (${busy ? "background/idle priority" : "normal priority"}, threads=${threads})`);

  try {
    await new Promise((resolve, reject) => {
      const proc = spawnPrioritized("ffmpeg", [
        "-i", localPath,
        "-vf", "scale='min(1920,iw)':'min(1080,ih)':force_original_aspect_ratio=decrease:force_divisible_by=2,fps=30",
        "-c:v", "libx264", "-preset", busy ? "ultrafast" : "veryfast",
        "-pix_fmt", "yuv420p", "-profile:v", "high",
        "-r", "30", "-g", "60", "-keyint_min", "60", "-sc_threshold", "0",
        "-force_key_frames", "expr:gte(t,n_forced*2)",
        "-crf", "23", "-maxrate", "6000k", "-bufsize", "12000k",
        "-threads", String(threads),
        "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
        "-movflags", "+faststart",
        "-y", outPath,
      ], busy);
      job.proc = proc;
      let settled = false;
      const finish = (error) => {
        if (settled) return;
        settled = true;
        if (job.proc === proc) job.proc = null;
        error ? reject(error) : resolve();
      };
      let stderr = "";
      proc.stderr.on("data", d => { stderr += d.toString(); });
      proc.on("exit", code => code === 0 ? finish() : finish(new Error(`ffmpeg exited ${code}: ${stderr.slice(-300)}`)));
      proc.on("error", finish);
    });
    if (job.cancelled) {
      try { fs.unlinkSync(outPath); } catch {}
      return;
    }
    fs.renameSync(outPath, localPath); // cached file is now normalized H.264 — future streams get instant copy-mode
    touchCacheFile(localPath);
    console.log(`[process] ${videoId} transcoded to h264 successfully`);
    if (job.cancelled) return;
    notifyUrl(callbackUrl, vpsKey, { video_id: videoId, ok: true, codec: "h264", transcoded: true });
  } catch (e) {
    try { fs.unlinkSync(outPath); } catch {}
    if (job.cancelled) return;
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
function safePath(p) {
  const raw = String(p || ".");
  const target = path.resolve(path.isAbsolute(raw) ? raw : path.join(ADMIN_SCOPE, raw));
  let ancestor = target;
  while (!fs.existsSync(ancestor) && ancestor !== path.dirname(ancestor)) {
    ancestor = path.dirname(ancestor);
  }
  const resolved = path.join(
    fs.realpathSync(ancestor),
    path.relative(ancestor, target),
  );
  const relative = path.relative(ADMIN_SCOPE, resolved);
  if (relative.startsWith("..") || path.isAbsolute(relative)) throw new Error("Path is outside the admin scope");
  return resolved;
}

app.get("/health", (_req, res) => res.json({ ok: true }));
app.get("/stats", auth, (_req, res) => res.json({ ...getStats(), processing_jobs: activeProcessingCount() }));

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

function waitForChildExit(child, timeoutMs = 5000) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return Promise.resolve();
  return new Promise((resolve) => {
    let finished = false;
    let timer;
    let killTimer;
    const finish = () => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      clearTimeout(killTimer);
      child.removeListener("exit", finish);
      child.removeListener("close", finish);
      child.removeListener("error", finish);
      resolve();
    };
    child.once("exit", finish);
    child.once("close", finish);
    child.once("error", finish);
    timer = setTimeout(() => {
      try { child.kill("SIGKILL"); } catch {}
      killTimer = setTimeout(finish, 1000);
    }, timeoutMs);
  });
}

function clearManagedCache() {
  const configuredPath = path.resolve(CACHE_DIR);
  const homePath = path.resolve(os.homedir());
  const adminPath = path.resolve(ADMIN_SCOPE);
  const tempPath = path.resolve(os.tmpdir());
  const rootPath = path.parse(configuredPath).root;
  const pathDepth = configuredPath.split(path.sep).filter(Boolean).length;

  if (configuredPath === rootPath || configuredPath === homePath ||
      configuredPath === adminPath || configuredPath === tempPath ||
      pathDepth < 2 ||
      homePath.startsWith(`${configuredPath}${path.sep}`) ||
      adminPath.startsWith(`${configuredPath}${path.sep}`) ||
      configuredPath.startsWith(`${adminPath}${path.sep}`) ||
      tempPath.startsWith(`${configuredPath}${path.sep}`)) {
    throw new Error("Configured cache directory is too broad or overlaps protected AutoLive files");
  }

  const stat = fs.lstatSync(configuredPath);
  if (!stat.isDirectory() || stat.isSymbolicLink()) {
    throw new Error("Configured cache path must be a real directory");
  }
  const realCachePath = fs.realpathSync(configuredPath);
  if (realCachePath !== configuredPath) {
    throw new Error("Configured cache path resolves through a symlink");
  }

  const names = fs.readdirSync(realCachePath);
  for (const name of names) {
    fs.rmSync(path.join(realCachePath, name), { recursive: true, force: true });
  }
  return names.length;
}

app.post("/admin/purge", auth, async (_req, res) => {
  if (isPurging) return res.status(409).json({ error: "AutoLive cleanup is already in progress" });
  isPurging = true;
  try {
    const activeStreams = [...streams.entries()];
    const jobs = [...processingJobs.values()];
    const children = new Set([
      ...activeStreams.map(([, entry]) => entry.proc).filter(Boolean),
      ...jobs.map((job) => job.proc).filter(Boolean),
      ...activeDownloads.values(),
    ]);

    for (const [slotId] of activeStreams) stopStream(slotId);
    for (const job of jobs) {
      job.cancelled = true;
      if (job.proc) {
        children.add(job.proc);
        try { job.proc.kill("SIGTERM"); } catch {}
      }
    }
    for (const child of activeDownloads.values()) {
      children.add(child);
      try { child.kill("SIGTERM"); } catch {}
    }

    const exits = Promise.all([...children].map((child) => waitForChildExit(child)));
    await exits;
    await Promise.allSettled([...downloadInFlight.values()]);
    let jobWaitTimer;
    const jobsFinished = await Promise.race([
      Promise.allSettled(jobs.map((job) => job.done).filter(Boolean)).then(() => true),
      new Promise((resolve) => {
        jobWaitTimer = setTimeout(() => resolve(false), 22000);
      }),
    ]);
    clearTimeout(jobWaitTimer);
    if (!jobsFinished) {
      return res.status(504).json({ error: "AutoLive tasks are still stopping; retry cleanup shortly" });
    }

    const statePath = path.resolve(STATE_FILE);
    const stateName = path.basename(statePath).toLowerCase();
    const adminPath = path.resolve(ADMIN_SCOPE);
    const rootPath = path.parse(statePath).root;
    if (statePath === rootPath || statePath === path.resolve(os.homedir()) ||
        stateName === ".env" || stateName.startsWith(".env.") ||
        statePath === adminPath || statePath.startsWith(`${adminPath}${path.sep}`)) {
      throw new Error("Configured stream-state path is protected and was not removed");
    }

    const cacheFilesRemoved = clearManagedCache();
    const stateRemoved = fs.existsSync(statePath);
    fs.rmSync(statePath, { force: true });
    fs.rmSync(`${statePath}.tmp`, { force: true });

    let tempPlaylistFilesRemoved = 0;
    for (const name of fs.readdirSync(os.tmpdir())) {
      if (!/^alp_playlist_[A-Za-z0-9_-]+\.txt$/.test(name)) continue;
      fs.rmSync(path.join(os.tmpdir(), name), { force: true });
      tempPlaylistFilesRemoved += 1;
    }

    console.log(`[purge] stopped ${activeStreams.length} streams and cleared managed AutoLive state/cache`);
    return res.json({
      ok: true,
      stopped_streams: activeStreams.length,
      cache_files_removed: cacheFilesRemoved,
      state_removed: stateRemoved,
      temp_playlist_files_removed: tempPlaylistFilesRemoved,
    });
  } catch (error) {
    console.error("[purge] AutoLive cleanup failed:", error.message);
    return res.status(500).json({ error: "Could not safely clear AutoLive-managed VPS data" });
  } finally {
    isPurging = false;
  }
});

function queueStreamStart(req, res) {
  if (isPurging) return res.status(503).json({ error: "AutoLive cleanup is in progress" });
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
  persistStreams();
  startFfmpeg(slot_id, cfg);

  console.log(`[${slot_id}] stream queued (${isPlaylist ? `playlist ${video_urls.length} videos` : "single video"})`);
  res.json({ ok: true, slot_id, mode: "smart-copy" });
}

app.post("/stream/start", auth, queueStreamStart);
app.post("/stream/start-playlist", auth, (req, res) => {
  const videos = Array.isArray(req.body?.videos) ? req.body.videos : [];
  if (!videos.length || videos.some((video) => !video || typeof video.url !== "string")) {
    return res.status(400).json({ error: "videos array with valid URLs required" });
  }
  req.body.video_urls = videos.map((video) => video.url);
  delete req.body.video_url;
  return queueStreamStart(req, res);
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
  if (/[;&|><`$()]|\.\.(?:\/|\\)/.test(String(cmd))) {
    return res.status(400).json({ error: "Shell operators and parent-directory paths are not allowed" });
  }
  const parts = String(cmd).trim().split(/\s+/);
  const allowed = new Set(["pwd", "ls", "find", "du", "df", "cat", "head", "tail", "stat", "mkdir", "touch"]);
  if (!allowed.has(parts[0])) return res.status(403).json({ error: "Command is not allowed in the scoped terminal" });
  if (parts.slice(1).some((arg) => arg.startsWith("-"))) {
    return res.status(400).json({ error: "Command options are disabled in the scoped terminal" });
  }
  const args = parts.slice(1).map((arg) => {
    return safePath(arg);
  });
  try {
    const executable = parts[0] === "pwd" ? "/bin/pwd" : `/usr/bin/${parts[0]}`;
    const stdout = execFileSync(executable, args, { cwd: ADMIN_SCOPE, timeout: 25000, maxBuffer: 2 * 1024 * 1024 }).toString();
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
  try {
    const dir = safePath(p);
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
  try {
    const target = safePath(p);
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
  try {
    const target = safePath(p);
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
    if (target === ADMIN_SCOPE) return res.status(400).json({ error: "The admin scope cannot be deleted" });
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
    try { if (req.file?.path) fs.unlinkSync(req.file.path); } catch {}
    return res.status(401).json({ error: "Invalid PIN" });
  }
  if (!req.file) return res.status(400).json({ error: "file required" });
  try {
    const target = path.join(safePath(dest), path.basename(req.file.originalname));
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

function restorePersistedStreams() {
  let saved;
  try {
    const stored = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    if (stored?.version !== 1) return;
    const decipher = crypto.createDecipheriv("aes-256-gcm", STATE_KEY, Buffer.from(stored.iv, "base64"));
    decipher.setAuthTag(Buffer.from(stored.tag, "base64"));
    saved = JSON.parse(Buffer.concat([
      decipher.update(Buffer.from(stored.data, "base64")),
      decipher.final(),
    ]).toString("utf8"));
  } catch {
    return;
  }
  if (!Array.isArray(saved)) return;
  for (const item of saved) {
    const cfg = item?.cfg;
    if (!cfg?.slot_id || !cfg?.platform || !cfg?.stream_key) continue;
    if (streams.has(cfg.slot_id)) continue;
    streams.set(cfg.slot_id, {
      cfg,
      proc: null,
      status: "starting",
      startedAt: null,
      restartCount: 0,
      stopTimer: null,
      useTranscode: false,
    });
    startFfmpeg(cfg.slot_id, cfg).catch((error) => {
      console.error(`[restore] ${cfg.slot_id} failed: ${error.message}`);
    });
  }
  persistStreams();
}

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

restorePersistedStreams();
