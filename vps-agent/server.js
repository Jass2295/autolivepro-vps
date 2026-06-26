require("dotenv").config();

const express   = require("express");
const { spawn } = require("child_process");
const crypto    = require("crypto");

const app      = express();
const PORT     = Number(process.env.PORT || 5001);
const API_KEY  = process.env.API_KEY || "";
const MAX_STREAMS   = Number(process.env.MAX_STREAMS || 5);
const MAX_RESTARTS  = Number(process.env.MAX_RESTARTS || 10); // circuit breaker

app.use(express.json());

// ─── Auth — FAIL-CLOSED ──────────────────────────────────────────────────────
if (!API_KEY) {
  console.error("FATAL: API_KEY is not set in .env — refusing to start");
  process.exit(1);
}

function safeEqual(a, b) {
  try {
    return crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b));
  } catch {
    return false;
  }
}

app.use((req, res, next) => {
  const key = req.headers["x-api-key"] || "";
  if (safeEqual(key, API_KEY)) return next();
  res.status(401).json({ ok: false, error: "Unauthorized" });
});

// ─── Stream state ─────────────────────────────────────────────────────────────
// Map: slot_id → { proc, config, status, startedAt, restartCount, stopTimer }
const streams = new Map();

// ─── Helpers ─────────────────────────────────────────────────────────────────
function rtmpUrl(platform, streamKey) {
  if (platform === "youtube")
    return `rtmp://a.rtmp.youtube.com/live2/${streamKey}`;
  if (platform === "facebook")
    return `rtmps://live-api-s.facebook.com:443/rtmp/${streamKey}`;
  return streamKey; // custom — streamKey IS the full URL
}

function buildFfmpegArgs(cfg) {
  const { video_url, platform, stream_key, loop } = cfg;
  const dest = rtmpUrl(platform, stream_key);
  const args = ["-hide_banner", "-loglevel", "warning", "-re"];
  if (loop) args.push("-stream_loop", "-1");
  args.push("-i", video_url, "-c:v", "copy", "-c:a", "copy", "-f", "flv", dest);
  return args;
}

/** Clean up a stream entry completely from the map. */
function cleanupEntry(slotId) {
  const entry = streams.get(slotId);
  if (!entry) return;
  if (entry.stopTimer) { clearTimeout(entry.stopTimer); entry.stopTimer = null; }
  entry.proc = null;
  streams.delete(slotId);
}

function startFfmpeg(slotId, cfg, restartCount = 0) {
  const entry = streams.get(slotId);
  if (!entry) return; // was stopped before we got here

  const args = buildFfmpegArgs(cfg);
  console.log(`[${slotId}] FFmpeg start (attempt ${restartCount + 1}):`, args.slice(-4).join(" "));

  const proc = spawn("ffmpeg", args, { stdio: ["ignore", "pipe", "pipe"] });

  // ── Spawn error (ffmpeg not found, permission denied, etc.) ──
  proc.on("error", (err) => {
    console.error(`[${slotId}] Spawn error: ${err.message}`);
    const current = streams.get(slotId);
    if (!current || current.proc !== proc) return;
    current.status = "error";
    cleanupEntry(slotId);
  });

  proc.stdout.on("data", d => process.stdout.write(`[${slotId}] ${d}`));
  proc.stderr.on("data", d => process.stderr.write(`[${slotId}] ${d}`));

  entry.proc         = proc;
  entry.startedAt    = new Date().toISOString();
  entry.restartCount = restartCount;
  entry.status       = "live";

  // Auto-stop timer
  if (cfg.stop_after_min > 0) {
    if (entry.stopTimer) clearTimeout(entry.stopTimer);
    entry.stopTimer = setTimeout(() => {
      console.log(`[${slotId}] Auto-stop after ${cfg.stop_after_min} min`);
      stopStream(slotId);
    }, cfg.stop_after_min * 60 * 1000);
  }

  // ── Process exit ──
  proc.on("exit", (code, signal) => {
    console.log(`[${slotId}] FFmpeg exited — code=${code} signal=${signal}`);
    const current = streams.get(slotId);
    if (!current || current.proc !== proc) return; // replaced or already cleaned

    const wasManual = (signal === "SIGTERM" || signal === "SIGKILL" || current.status === "stopped");

    if (!wasManual && cfg.loop) {
      // Heartbeat: bounded restart with circuit breaker
      if (restartCount >= MAX_RESTARTS) {
        console.error(`[${slotId}] Circuit breaker: ${MAX_RESTARTS} restarts reached — giving up`);
        current.status = "error";
        cleanupEntry(slotId);
        return;
      }
      const delay = Math.min(5000 * (restartCount + 1), 30000); // 5s → 30s backoff
      console.log(`[${slotId}] Restarting in ${delay / 1000}s (${restartCount + 1}/${MAX_RESTARTS})...`);
      current.status = "restarting";
      current.proc   = null;
      setTimeout(() => {
        const still = streams.get(slotId);
        if (still && still.status === "restarting") {
          startFfmpeg(slotId, cfg, restartCount + 1);
        }
      }, delay);
    } else {
      // Terminal exit — clean up completely so capacity is freed
      cleanupEntry(slotId);
    }
  });
}

function stopStream(slotId) {
  const entry = streams.get(slotId);
  if (!entry) return false;
  entry.status = "stopped"; // mark before kill so exit handler won't restart
  const proc = entry.proc;
  cleanupEntry(slotId);
  if (proc) {
    try { proc.kill("SIGTERM"); } catch {}
    setTimeout(() => { try { proc.kill("SIGKILL"); } catch {} }, 5000);
  }
  return true;
}

// ─── Routes ──────────────────────────────────────────────────────────────────

// GET /health
app.get("/health", (_req, res) => {
  res.json({ ok: true, time: new Date().toISOString(), active_streams: streams.size });
});

// GET /stream/status
app.get("/stream/status", (_req, res) => {
  const list = [];
  for (const [slot_id, e] of streams) {
    list.push({
      slot_id,
      status:         e.status,
      platform:       e.config.platform,
      loop:           e.config.loop,
      stop_after_min: e.config.stop_after_min,
      started_at:     e.startedAt,
      restart_count:  e.restartCount,
      pid:            e.proc ? e.proc.pid : null,
    });
  }
  res.json({ ok: true, active: list.length, max: MAX_STREAMS, streams: list });
});

// POST /stream/start
app.post("/stream/start", (req, res) => {
  const {
    slot_id, video_url, platform, stream_key,
    loop = true, stop_after_min = 0,
  } = req.body || {};

  if (!slot_id || !video_url || !platform || !stream_key)
    return res.status(400).json({ ok: false, error: "slot_id, video_url, platform, stream_key required" });

  if (!["youtube", "facebook", "custom"].includes(platform))
    return res.status(400).json({ ok: false, error: "platform must be youtube, facebook or custom" });

  // Replace existing stream for this slot if any
  if (streams.has(slot_id)) {
    console.log(`[${slot_id}] Replacing existing stream`);
    stopStream(slot_id);
  }

  // Check capacity — only count entries that have a live proc or are pending
  if (streams.size >= MAX_STREAMS)
    return res.status(429).json({ ok: false, error: `VPS at capacity (max ${MAX_STREAMS} concurrent streams)` });

  const cfg = { video_url, platform, stream_key, loop, stop_after_min };

  // Register entry BEFORE spawning
  streams.set(slot_id, {
    proc: null, config: cfg, status: "starting",
    startedAt: new Date().toISOString(), restartCount: 0, stopTimer: null,
  });

  startFfmpeg(slot_id, cfg, 0);
  res.json({ ok: true, slot_id, message: "Stream starting — going live in ~10s" });
});

// POST /stream/stop
app.post("/stream/stop", (req, res) => {
  const { slot_id } = req.body || {};
  if (!slot_id) return res.status(400).json({ ok: false, error: "slot_id required" });
  const stopped = stopStream(slot_id);
  res.json({ ok: true, stopped, slot_id });
});

// ─── Start ───────────────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n🎬  AutoLive Pro VPS Agent — port ${PORT}`);
  console.log(`   Max streams: ${MAX_STREAMS} | Circuit breaker: ${MAX_RESTARTS} retries\n`);
});

// Graceful shutdown — stop all active ffmpeg processes
process.on("SIGTERM", () => {
  console.log("Shutting down — stopping all streams...");
  for (const id of [...streams.keys()]) stopStream(id);
  process.exit(0);
});
