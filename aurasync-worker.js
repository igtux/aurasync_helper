#!/usr/bin/env node
'use strict';
/**
 * AuraSync remote transcoding worker.
 *
 * A single-file Node.js CLI that polls an AuraSync server for transcode jobs
 * and runs them locally, using hardware-accelerated ffmpeg if present. Zero
 * runtime dependencies — only Node 18+ built-ins (fetch, fs, child_process).
 *
 * Usage:
 *   node aurasync-worker.js
 *     (reads ./worker.config.json or env AURASYNC_SERVER + AURASYNC_WORKER_TOKEN)
 *
 *   aurasync-worker --server https://aurasync.erpaura.ge --token aw_... --name my-rig
 *
 * First run: generate a token in the admin panel → paste it here. The worker
 * creates worker.config.json next to the binary so it remembers the config
 * for next time.
 *
 * Stages for one job:
 *   1. CLAIM: POST /api/workers/claim → get source URL + output prefix + ladder
 *   2. DOWNLOAD: stream the R2 source into a local temp file
 *   3. ENCODE: spawn ffmpeg (3-rung HLS ladder) — hw encoder if available
 *   4. UPLOAD: for each produced file, ask server for presigned PUT → PUT to R2
 *   5. COMPLETE: POST /api/workers/jobs/:id/complete → server flips title live
 */

const fs = require('fs');
const path = require('path');
const os = require('os');
const { spawn, spawnSync } = require('child_process');

// ---------- config ----------

const HERE = path.dirname(process.argv[1] || process.cwd());
const CONFIG_PATH = process.env.AURASYNC_WORKER_CONFIG || path.join(HERE, 'worker.config.json');

function parseArgs() {
  const out = { positional: [] };
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--server') out.server = argv[++i];
    else if (a === '--token') out.token = argv[++i];
    else if (a === '--name') out.name = argv[++i];
    else if (a === '--ffmpeg') out.ffmpeg = argv[++i];
    else if (a === '--ffprobe') out.ffprobe = argv[++i];
    else if (a === '--tmp') out.tmpDir = argv[++i];
    else if (a === '--no-hw') out.noHw = true;
    else if (a === '--title') out.titleId = argv[++i];
    else if (a === '--episode') out.episodeId = argv[++i];
    else if (a === '--port') out.port = Number(argv[++i]);
    else if (a === '--inbox') out.inbox = argv[++i];
    else if (a === '--help' || a === '-h') { printHelp(); process.exit(0); }
    else if (!a.startsWith('-')) out.positional.push(a);
  }
  return out;
}

function printHelp() {
  console.log(`
AuraSync remote transcoding worker.

Usage:
  aurasync-worker [run]              # default: poll the server for queued jobs
  aurasync-worker requests           # list pending title requests on the server
  aurasync-worker ingest <file>      # transcode a LOCAL file → HLS → R2 (no round-trip)
    --title <titleId>                #   map to catalogue title (required)
    --episode <episodeId>            #   map to episode (optional; series only)
  aurasync-worker scan <folder>      # dry-run: show what files in <folder> would match
    --title <titleId>                #   scope matches to one title (optional)
  aurasync-worker ingest-folder <folder>
                                     # walk <folder>, match files to episodes, queue each
                                     #   serially. Parses SxxExx + parent-dir-season; with
                                     #   --title it can also resolve absolute-episode nums
                                     #   (e.g. "Naruto 207.mkv" → S9E13).
    --title <titleId>                #   scope matches to one title (optional)
  aurasync-worker gui                # local browser GUI for picking + fulfilling requests
    --port <port>                    #   default 4849
    --inbox <dir>                    #   folder of local video files to choose from

Global options:
  --server <url>      AuraSync server base URL (e.g. https://aurasync.erpaura.ge)
  --token <token>     Worker token minted in the admin panel (shown once)
  --name <name>       Human-readable worker name (default: hostname)
  --ffmpeg <path>     Path to ffmpeg binary (default: "ffmpeg" on PATH)
  --ffprobe <path>    Path to ffprobe binary (default: "ffprobe" on PATH)
  --tmp <dir>         Working directory for downloads+encoding (default: OS tmpdir)
  --no-hw             Force software encoding (libx264) even if GPU encoder present
  --help              Show this help

Config is persisted to worker.config.json next to this script on first run.
Environment fallbacks: AURASYNC_SERVER, AURASYNC_WORKER_TOKEN.

Examples:
  aurasync-worker
    Poll mode. Claim jobs queued on the server (source on R2).

  aurasync-worker requests
    Print a numbered list of pending title requests.

  aurasync-worker ingest "D:\\Movies\\Pirates.mkv" --title 6a2df9af-...
    Transcode the local file right here, upload only HLS to R2.
    No re-download of the source from R2.
`);
}

function loadConfig() {
  const cli = parseArgs();
  let disk = {};
  try {
    if (fs.existsSync(CONFIG_PATH)) {
      disk = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
    }
  } catch (err) {
    console.warn(`[warn] could not read ${CONFIG_PATH}: ${err.message}`);
  }
  const merged = {
    server: cli.server || disk.server || process.env.AURASYNC_SERVER,
    token: cli.token || disk.token || process.env.AURASYNC_WORKER_TOKEN,
    name: cli.name || disk.name || os.hostname() || 'aurasync-worker',
    ffmpeg: cli.ffmpeg || disk.ffmpeg || process.env.FFMPEG_PATH || 'ffmpeg',
    ffprobe: cli.ffprobe || disk.ffprobe || process.env.FFPROBE_PATH || 'ffprobe',
    tmpDir: cli.tmpDir || disk.tmpDir || path.join(os.tmpdir(), 'aurasync-worker'),
    noHw: !!(cli.noHw || disk.noHw || process.env.AURASYNC_NO_HW),
    // gui-only fields — ignored in poll/ingest modes
    port: cli.port || disk.port || 4849,
    inbox: cli.inbox || disk.inbox || null,
  };
  if (!merged.server || !merged.token) {
    console.error('Missing --server and/or --token. See --help.');
    process.exit(1);
  }
  // Strip trailing slash
  merged.server = String(merged.server).replace(/\/+$/, '');
  // Persist for next run. We merge-write so other tools (e.g. the GUI) can
  // add fields we don't know about here.
  try {
    const persisted = {
      ...disk,
      server: merged.server, token: merged.token, name: merged.name,
      ffmpeg: merged.ffmpeg, ffprobe: merged.ffprobe, tmpDir: merged.tmpDir,
      noHw: merged.noHw,
    };
    // Only write inbox/port if explicitly set (don't overwrite with null).
    if (merged.inbox != null) persisted.inbox = merged.inbox;
    if (merged.port != null) persisted.port = merged.port;
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(persisted, null, 2));
  } catch (err) {
    console.warn(`[warn] could not persist config to ${CONFIG_PATH}: ${err.message}`);
  }
  return merged;
}

// ---------- logging ----------

function ts() {
  const d = new Date();
  return d.toTimeString().slice(0, 8);
}
function log(...a) { console.log(`[${ts()}]`, ...a); }
function warn(...a) { console.warn(`[${ts()}] WARN`, ...a); }
function err(...a) { console.error(`[${ts()}] ERROR`, ...a); }

// ---------- capabilities: ffmpeg + GPU encoder detection ----------

function detectCapabilities(cfg) {
  const caps = {
    os: process.platform,
    arch: process.arch,
    cpus: os.cpus().length,
    memoryMB: Math.round(os.totalmem() / 1024 / 1024),
    nodeVersion: process.version,
    ffmpegVersion: null,
    encoders: [],
    picked: 'libx264',
  };
  // ffmpeg present?
  try {
    const r = spawnSync(cfg.ffmpeg, ['-version'], { encoding: 'utf8', timeout: 5000 });
    if (r.status === 0) {
      const firstLine = (r.stdout || '').split('\n')[0] || '';
      caps.ffmpegVersion = firstLine.trim();
    }
  } catch { /* ffmpeg missing */ }
  if (!caps.ffmpegVersion) {
    err(`ffmpeg not runnable at "${cfg.ffmpeg}". Install ffmpeg and/or pass --ffmpeg <path>.`);
    process.exit(2);
  }
  // ffmpeg -encoders — look for hardware H.264 encoders
  try {
    const r = spawnSync(cfg.ffmpeg, ['-hide_banner', '-encoders'], { encoding: 'utf8', timeout: 5000 });
    const text = (r.stdout || '') + '\n' + (r.stderr || '');
    const findings = [];
    if (/\bh264_nvenc\b/.test(text)) findings.push('h264_nvenc');
    if (/\bh264_qsv\b/.test(text)) findings.push('h264_qsv');
    if (/\bh264_amf\b/.test(text)) findings.push('h264_amf');
    if (/\bh264_videotoolbox\b/.test(text)) findings.push('h264_videotoolbox');
    if (/\blibx264\b/.test(text)) findings.push('libx264');
    caps.encoders = findings;
  } catch { /* fall through */ }

  // Pick the best encoder available. User can force software with --no-hw.
  if (!cfg.noHw) {
    for (const pref of ['h264_nvenc', 'h264_qsv', 'h264_amf', 'h264_videotoolbox']) {
      if (caps.encoders.includes(pref)) { caps.picked = pref; break; }
    }
  }
  return caps;
}

// ---------- API client ----------

function makeApi(cfg) {
  const base = cfg.server;
  async function call(method, pathName, body) {
    const url = `${base}${pathName}`;
    const init = {
      method,
      headers: {
        'Authorization': `Bearer ${cfg.token}`,
        ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
      },
    };
    if (body !== undefined) init.body = JSON.stringify(body);
    const res = await fetch(url, init);
    const text = await res.text();
    let json = null;
    try { json = text ? JSON.parse(text) : null; } catch { /* non-JSON */ }
    if (!res.ok) {
      const reason = (json && json.error) || text.slice(0, 200) || `HTTP ${res.status}`;
      const e = new Error(`${method} ${pathName} → ${res.status}: ${reason}`);
      e.status = res.status;
      throw e;
    }
    return json;
  }
  return {
    register: (capabilities) => call('POST', '/api/workers/register', { capabilities }),
    claim: (capabilities) => call('POST', '/api/workers/claim', { capabilities }),
    heartbeat: (capabilities) => call('POST', '/api/workers/heartbeat', { capabilities }),
    progress: (jobId, p) => call('POST', `/api/workers/jobs/${encodeURIComponent(jobId)}/progress`, { progress: p }),
    uploadUrl: (jobId, p) => call('POST', `/api/workers/jobs/${encodeURIComponent(jobId)}/upload-url`, { path: p }),
    complete: (jobId, details) => call('POST', `/api/workers/jobs/${encodeURIComponent(jobId)}/complete`, details || {}),
    fail: (jobId, reason, permanent = false) =>
      call('POST', `/api/workers/jobs/${encodeURIComponent(jobId)}/fail`, { error: String(reason), permanent: !!permanent }),
    listRequests: () => call('GET', '/api/workers/requests'),
    ingestLocal: (body) => call('POST', '/api/workers/ingest-local', body),
    // v2: fetch a title's full episode list so we can (a) ingest episodes
    // without a pending request and (b) decode absolute-episode numbers.
    listEpisodes: (titleId) => call('GET', `/api/workers/titles/${encodeURIComponent(titleId)}/episodes`),
  };
}

// ---------- source download (streams R2 → local file) ----------

async function downloadSource(url, destPath, onProgress) {
  log(`downloading source → ${destPath}`);
  fs.mkdirSync(path.dirname(destPath), { recursive: true });
  const res = await fetch(url);
  if (!res.ok || !res.body) throw new Error(`source GET ${res.status}`);
  const total = Number(res.headers.get('content-length') || 0);
  const out = fs.createWriteStream(destPath);
  let got = 0;
  let lastReport = 0;
  const reader = res.body.getReader();
  try {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      out.write(value);
      got += value.length;
      if (onProgress && Date.now() - lastReport > 1000) {
        lastReport = Date.now();
        onProgress(got, total);
      }
    }
  } finally {
    out.end();
    await new Promise((resolve) => out.on('close', resolve));
  }
  const st = fs.statSync(destPath);
  return { bytes: st.size };
}

// ---------- ffmpeg: probe + transcode ----------

function ffprobeDurationSec(cfg, srcPath) {
  try {
    const r = spawnSync(cfg.ffprobe, [
      '-v', 'error',
      '-show_entries', 'format=duration',
      '-of', 'default=nw=1:nk=1',
      srcPath,
    ], { encoding: 'utf8', timeout: 10_000 });
    const n = Number((r.stdout || '').trim());
    return Number.isFinite(n) && n > 0 ? n : null;
  } catch {
    return null;
  }
}

// Shared tail: HLS muxer output. One variant (1080p) → one master + one media
// playlist. We keep the master + v1080p/ layout even for a single rendition
// so the server-side playlist rewriter doesn't need to special-case it.
//
// IMPORTANT: on Windows, path.join uses '\'. ffmpeg writes whatever separator
// is in its argv straight into the emitted master playlist — so we'd end up
// with `v1080p\index.m3u8` as a variant URI, which iOS's native HLS player
// rejects (Chrome's hls.js normalizes it). Force POSIX separators here;
// Windows ffmpeg accepts '/' for disk paths just fine.
function posixJoin(...parts) {
  return parts.join('/').replace(/[\\/]+/g, '/');
}
function hlsMuxerTail(outDir) {
  const pOut = String(outDir).replace(/\\/g, '/');
  return [
    '-g', '48', '-keyint_min', '48',
    '-c:a', 'aac', '-ar', '48000', '-b:a', '128k',
    '-f', 'hls',
    '-hls_time', '6',
    '-hls_playlist_type', 'vod',
    '-hls_flags', 'independent_segments',
    '-hls_segment_type', 'mpegts',
    '-hls_segment_filename', posixJoin(pOut, 'v%v', 'seg_%05d.ts'),
    '-master_pl_name', 'master.m3u8',
    '-var_stream_map', 'v:0,a:0,name:1080p',
    posixJoin(pOut, 'v%v', 'index.m3u8'),
  ];
}

/**
 * Fully-GPU pipeline for NVENC at 1080p: decode → scale_cuda → encode.
 * Frames never leave VRAM. On a consumer GPU (RTX 3080 / 4070 class) this
 * runs 3-5× faster than the PCIe-bouncing path because:
 *
 *   - without `-hwaccel_output_format cuda`, ffmpeg copies each decoded
 *     frame to host RAM, does CPU swscale, then uploads to NVENC. CPU +
 *     PCIe are the bottleneck even though NVENC itself is idle.
 *
 *   - WITH cuda output format + scale_cuda, the surface never touches
 *     host memory. NVENC consumes CUDA frames directly.
 *
 * Output is 1080p height-clamped preserving aspect ratio (`-2` keeps width
 * even-divisible). Sources smaller than 1080p are upscaled — that's rare
 * and transient (admins don't usually fulfil with 720p sources).
 *
 * Requires ffmpeg built with CUDA + scale_cuda. Gyan's Windows
 * "essentials" and "full" builds both include it, as do the static Linux
 * builds shipped by johnvansickle.
 */
function buildNvencArgs(srcPath, outDir) {
  return [
    '-y', '-hide_banner', '-loglevel', 'error',
    '-progress', 'pipe:2', '-nostats',

    // GPU decode AND keep decoded surfaces in VRAM.
    '-hwaccel', 'cuda',
    '-hwaccel_output_format', 'cuda',

    '-i', srcPath,

    // Scale DOWN to 1080p when source is larger; otherwise passthrough.
    //   * 720p input (TBBT-style BluRay rips): stays 1280×720 — no pointless
    //     upscale, no extra encoder work, no inflated output file.
    //   * 1080p input: stays 1920×1080.
    //   * 2160p (4K) input: scales to 1080p height, width auto.
    // The `\,` escape is mandatory: commas inside filter option values have
    // to be escaped so they aren't read as a filter separator.
    '-vf', 'scale_cuda=-2:min(1080\\,ih)',
    '-map', '0:v:0', '-map', '0:a:0?',

    '-c:v', 'h264_nvenc',
    // p1 = fastest NVENC preset. On RTX 30xx at 5 Mbps 1080p, PSNR vs p4 is
    // within ~0.3 dB (invisible) but throughput is ~2× higher. Right call
    // for VOD streaming-ceiling bitrates.
    '-preset', 'p1',
    '-rc', 'vbr',
    '-b:v', '5000k', '-maxrate', '5350k', '-bufsize', '7500k',

    ...hlsMuxerTail(outDir),
  ];
}

/** Intel QSV equivalent of the NVENC pipeline. scale_qsv + h264_qsv. */
function buildQsvArgs(srcPath, outDir) {
  return [
    '-y', '-hide_banner', '-loglevel', 'error',
    '-progress', 'pipe:2', '-nostats',

    '-hwaccel', 'qsv',
    '-hwaccel_output_format', 'qsv',

    '-i', srcPath,

    '-vf', 'scale_qsv=-2:min(1080\\,ih)',
    '-map', '0:v:0', '-map', '0:a:0?',

    '-c:v', 'h264_qsv',
    '-preset', 'veryfast',
    '-b:v', '5000k', '-maxrate', '5350k', '-bufsize', '7500k',

    ...hlsMuxerTail(outDir),
  ];
}

/**
 * Fallback software pipeline — used for libx264, h264_amf, h264_videotoolbox,
 * and anywhere the GPU-resident filters aren't available. swscale on CPU,
 * encoder as chosen. h264_amf / h264_videotoolbox still benefit from the
 * GPU encode; just no zero-copy scale.
 */
function buildSoftwareScaleArgs(srcPath, outDir, enc) {
  const isSw = enc === 'libx264';
  const preset = isSw ? 'veryfast'
    : enc === 'h264_amf' ? 'speed'
    : null; // videotoolbox has no preset
  const args = [
    '-y', '-hide_banner', '-loglevel', 'error',
    '-progress', 'pipe:2', '-nostats',
    '-i', srcPath,
    '-vf', 'scale=-2:min(1080\\,ih)',
    '-map', '0:v:0', '-map', '0:a:0?',
    '-c:v', enc,
  ];
  if (preset) args.push('-preset', preset);
  if (isSw) args.push('-profile:v', 'main');
  args.push(
    '-pix_fmt', 'yuv420p',
    '-sc_threshold', '0',
    '-b:v', '5000k', '-maxrate', '5350k', '-bufsize', '7500k',
    ...hlsMuxerTail(outDir),
  );
  return args;
}

function buildFfmpegArgs(cfg, srcPath, outDir, caps) {
  const enc = caps.picked || 'libx264';
  if (enc === 'h264_nvenc') return buildNvencArgs(srcPath, outDir);
  if (enc === 'h264_qsv') return buildQsvArgs(srcPath, outDir);
  // AMF, VideoToolbox, libx264 — CPU scale, encoder as chosen.
  return buildSoftwareScaleArgs(srcPath, outDir, enc);
}

function runFfmpegWithProgress(cfg, args, durationSec, onProgress, onSpawn) {
  return new Promise((resolve, reject) => {
    const child = spawn(cfg.ffmpeg, args, { stdio: ['ignore', 'ignore', 'pipe'] });
    if (typeof onSpawn === 'function') { try { onSpawn(child); } catch {} }
    let buf = '';
    let lastPct = 0;
    let lastReport = 0;
    child.stderr.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      // ffmpeg emits key=value pairs via -progress pipe:2
      while (true) {
        const nl = buf.indexOf('\n');
        if (nl === -1) break;
        const line = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        const eq = line.indexOf('=');
        if (eq === -1) continue;
        const k = line.slice(0, eq).trim();
        const v = line.slice(eq + 1).trim();
        if (k === 'out_time_ms' && durationSec > 0) {
          const sec = Number(v) / 1_000_000;
          const pct = Math.max(0, Math.min(1, sec / durationSec));
          if (pct - lastPct > 0.01 || Date.now() - lastReport > 3000) {
            lastPct = pct;
            lastReport = Date.now();
            try { onProgress(pct); } catch { /* ignore */ }
          }
        }
        if (k === 'progress' && v === 'end') {
          try { onProgress(1); } catch { /* ignore */ }
        }
      }
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ffmpeg exited ${code}`));
    });
  });
}

// ---------- upload (worker → server for PUT url → R2) ----------

async function uploadFile(api, jobId, localPath, relKey) {
  const { url, contentType } = await api.uploadUrl(jobId, relKey);
  const stat = fs.statSync(localPath);
  // Use Node Readable → Web ReadableStream via fetch's `duplex: 'half'` so we
  // don't slurp multi-GB segments into memory.
  const stream = fs.createReadStream(localPath);
  const res = await fetch(url, {
    method: 'PUT',
    headers: {
      'Content-Type': contentType,
      'Content-Length': String(stat.size),
    },
    body: stream,
    duplex: 'half',
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`PUT ${relKey} → ${res.status}: ${text.slice(0, 200)}`);
  }
  return { bytes: stat.size };
}

function walkFiles(dir) {
  const out = [];
  const stack = [dir];
  while (stack.length) {
    const cur = stack.pop();
    for (const entry of fs.readdirSync(cur, { withFileTypes: true })) {
      const p = path.join(cur, entry.name);
      if (entry.isDirectory()) stack.push(p);
      else if (entry.isFile()) out.push(p);
    }
  }
  return out;
}

/**
 * Upload every file under `hlsDir` to R2 using a bounded-concurrency pool.
 * Previously this loop was serial — at ~100 ms per round-trip that caps
 * throughput at ~6 Mbit/s regardless of link speed (one small segment per
 * RTT). With 12 in flight we saturate link bandwidth on anything up to
 * gigabit. Progress is tallied atomically as workers resolve.
 *
 * Env override: AURASYNC_UPLOAD_CONCURRENCY (default 12, clamp 1..32).
 */
async function uploadHlsTree(api, jobId, hlsDir, onProgress) {
  const files = walkFiles(hlsDir);
  const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));
  const concurrency = clamp(Number(process.env.AURASYNC_UPLOAD_CONCURRENCY) || 12, 1, 32);
  const n = Math.min(concurrency, files.length || 1);
  let cursor = 0;
  let done = 0;
  let totalBytes = 0;
  async function runner() {
    while (true) {
      const i = cursor++;
      if (i >= files.length) return;
      const abs = files[i];
      const rel = path.relative(hlsDir, abs).split(path.sep).join('/');
      const { bytes } = await uploadFile(api, jobId, abs, rel);
      totalBytes += bytes;
      done += 1;
      if (onProgress) { try { onProgress(done, files.length, totalBytes); } catch {} }
    }
  }
  await Promise.all(Array.from({ length: n }, () => runner()));
  return { totalBytes, fileCount: files.length };
}

// ---------- one-job runner ----------

async function runOneJob(api, cfg, job, caps) {
  const jobId = job.jobId;
  const workDir = path.join(cfg.tmpDir, jobId);
  fs.mkdirSync(workDir, { recursive: true });
  const srcExt = (path.extname(new URL(job.source.url).pathname).replace(/[?#].*$/, '') || '.mp4').toLowerCase();
  const srcPath = path.join(workDir, 'src' + srcExt);
  const hlsDir = path.join(workDir, 'hls');
  fs.mkdirSync(hlsDir, { recursive: true });
  for (const v of ['v1080p']) {
    fs.mkdirSync(path.join(hlsDir, v), { recursive: true });
  }

  // 1. Download source (counts 0 → 10% of job progress)
  await downloadSource(job.source.url, srcPath, (got, total) => {
    if (total > 0) {
      const pct = (got / total) * 0.10;
      api.progress(jobId, pct).catch(() => {});
    }
  });

  // 2. Probe + transcode (counts 10 → 90%)
  const durationSec = ffprobeDurationSec(cfg, srcPath);
  log(`probed duration=${durationSec || 'unknown'}s — encoder=${caps.picked}`);
  const args = buildFfmpegArgs(cfg, srcPath, hlsDir, caps);
  await runFfmpegWithProgress(cfg, args, durationSec || 0, (pct) => {
    const overall = 0.10 + (pct * 0.80);
    api.progress(jobId, overall).catch(() => {});
  });

  // 3. Upload everything under hlsDir → keyPrefix/<relative>, 12-way parallel.
  log(`uploading HLS tree (parallel)`);
  const { totalBytes, fileCount } = await uploadHlsTree(api, jobId, hlsDir, (done, total) => {
    const pct = 0.90 + (done / total) * 0.10;
    api.progress(jobId, Math.min(0.99, pct)).catch(() => {});
  });

  // 4. Tell server we're done → it flips the title to available.
  await api.complete(jobId, { durationSec, bytes: totalBytes });

  // 5. Clean up temp files.
  try { fs.rmSync(workDir, { recursive: true, force: true }); } catch { /* ignore */ }

  log(`job ${jobId.slice(0, 8)} complete (${fileCount} files, ${(totalBytes / 1024 / 1024).toFixed(1)} MB)`);
}

/**
 * Local-ingest variant: the source file is already on this machine. Skip the
 * R2 download and transcode in place. ffmpeg output still goes to a tmp dir,
 * then gets uploaded to R2. The local source is NEVER copied or uploaded.
 */
async function runLocalIngestJob(api, cfg, job, localSrcPath, caps) {
  const jobId = job.jobId;
  const workDir = path.join(cfg.tmpDir, jobId);
  fs.mkdirSync(workDir, { recursive: true });
  const hlsDir = path.join(workDir, 'hls');
  fs.mkdirSync(hlsDir, { recursive: true });
  for (const v of ['v1080p']) {
    fs.mkdirSync(path.join(hlsDir, v), { recursive: true });
  }

  // 1. (no download; source is local)
  // 2. Probe + transcode (counts 0 → 90% this time; skipped the 10% download band)
  const durationSec = ffprobeDurationSec(cfg, localSrcPath);
  log(`probed duration=${durationSec || 'unknown'}s — encoder=${caps.picked}`);
  const args = buildFfmpegArgs(cfg, localSrcPath, hlsDir, caps);
  await runFfmpegWithProgress(cfg, args, durationSec || 0, (pct) => {
    api.progress(jobId, pct * 0.90).catch(() => {});
  });

  // 3. Upload HLS output to R2 (12-way parallel).
  log(`uploading HLS tree (parallel)`);
  const { totalBytes, fileCount } = await uploadHlsTree(api, jobId, hlsDir, (done, total) => {
    const pct = 0.90 + (done / total) * 0.10;
    api.progress(jobId, Math.min(0.99, pct)).catch(() => {});
  });

  await api.complete(jobId, { durationSec, bytes: totalBytes });
  try { fs.rmSync(workDir, { recursive: true, force: true }); } catch { /* ignore */ }
  log(`local-ingest job ${jobId.slice(0, 8)} complete (${fileCount} files, ${(totalBytes / 1024 / 1024).toFixed(1)} MB)`);
}

// ---------- subcommands: requests + ingest ----------

async function cmdRequests(api) {
  const r = await api.listRequests();
  const rows = r.requests || [];
  if (rows.length === 0) {
    console.log('No pending title requests.');
    return;
  }
  console.log(`Pending requests (${rows.length}):\n`);
  for (const req of rows) {
    const kind = req.titleTmdbType || '?';
    const ep = req.episodeSeason
      ? ` — S${req.episodeSeason}E${req.episodeNumber}${req.episodeName ? ' · ' + req.episodeName : ''}`
      : '';
    const year = req.titleYear ? ` (${req.titleYear})` : '';
    console.log(`  ${req.titleTitle}${year}  [${kind}]${ep}`);
    console.log(`      title=${req.titleId}${req.episodeId ? '  episode=' + req.episodeId : ''}`);
    console.log(`      requested by ${req.requesterName || '?'}`);
    console.log('');
  }
  console.log('To fulfill one, run:');
  console.log('  aurasync-worker ingest <path/to/file> --title <titleId>' + (rows.some(r => r.episodeId) ? ' [--episode <episodeId>]' : ''));
}

async function cmdIngest(api, cfg, caps, filePath, titleId, episodeId) {
  if (!filePath) {
    console.error('Missing file path. Usage: aurasync-worker ingest <file> --title <uuid> [--episode <uuid>]');
    process.exit(2);
  }
  const abs = path.resolve(filePath);
  if (!fs.existsSync(abs)) {
    console.error(`File not found: ${abs}`);
    process.exit(2);
  }
  if (!titleId) {
    console.error('Missing --title <titleId>. Run `aurasync-worker requests` to list options.');
    process.exit(2);
  }
  const stat = fs.statSync(abs);
  if (!stat.isFile()) {
    console.error(`Not a regular file: ${abs}`);
    process.exit(2);
  }
  console.log(`local ingest: ${abs}  (${(stat.size / 1024 / 1024).toFixed(1)} MB)`);
  console.log(`target: title=${titleId}${episodeId ? '  episode=' + episodeId : ''}`);

  // Register first so the worker is known. (If already registered from a prior
  // poll-mode run, this is just an idempotent heartbeat.)
  try {
    await api.register(caps);
  } catch (e) {
    console.error(`register failed: ${e.message}`);
    process.exit(3);
  }

  // Ask server to pre-claim a transcode job for this (title, episode).
  let job;
  try {
    job = await api.ingestLocal({
      titleId,
      episodeId: episodeId || null,
      filename: path.basename(abs),
      bytes: stat.size,
    });
  } catch (e) {
    console.error(`ingest-local failed: ${e.message}`);
    process.exit(4);
  }
  console.log(`job ${job.jobId.slice(0, 8)} claimed; transcoding locally…`);

  try {
    await runLocalIngestJob(api, cfg, job, abs, caps);
    console.log(`\n✓ Done. Title should be 'available' in the catalogue momentarily.`);
  } catch (e) {
    console.error(`transcode failed: ${e.message}`);
    try { await api.fail(job.jobId, e.message, false); } catch { /* ignore */ }
    process.exit(5);
  }
}

// ---------- folder-ingest: scan + ingest-folder ----------

const VIDEO_EXT_RE = /\.(mp4|mkv|mov|webm|m4v|avi|ts|flv|wmv)$/i;

/**
 * Parse a video filename into { season, episode } or null.
 * Recognised patterns (in priority order):
 *   S01E02, s1e2, S01.E02, S01_E02         — explicit SxxExx
 *   Season 1 Episode 2, Season_01_Ep02     — verbose form
 *   1x02                                   — season x episode
 *   Episode 12, Ep12, EP.12                — season-less, assume 1
 *   E12, ep012                             — same
 *   Show_12.mkv                            — trailing number before ext (season 1)
 *
 * Absolute-episode numbers (e.g. "Naruto 207.mkv" meaning S9E13) are NOT
 * parsed here — that needs a server lookup and ships in v2.
 */
function parseEpisodeFromFilename(name) {
  const base = path.basename(name);
  let m = /\bS(\d{1,3})[._\s-]?E(\d{1,4})\b/i.exec(base);
  if (m) return { season: Number(m[1]), episode: Number(m[2]) };
  m = /season[\s._-]?(\d{1,3})[^\d]{0,10}(episode|ep)[\s._-]?(\d{1,4})/i.exec(base);
  if (m) return { season: Number(m[1]), episode: Number(m[3]) };
  m = /\b(\d{1,2})x(\d{1,4})\b/i.exec(base);
  if (m) return { season: Number(m[1]), episode: Number(m[2]) };
  m = /\b(?:episode|ep)[\s._-]?(\d{1,4})\b/i.exec(base);
  if (m) return { season: 1, episode: Number(m[1]) };
  m = /\bE(\d{1,4})\b/i.exec(base);
  if (m) return { season: 1, episode: Number(m[1]) };
  m = /[-_.\s](\d{1,4})(?=\.[a-z0-9]+$)/i.exec(base);
  if (m) return { season: 1, episode: Number(m[1]) };
  return null;
}

/**
 * Parent-dir-season parser. Walks up from the file's directory looking for a
 * segment like "Season 1", "s01", "S3", "Saison 2" — so filenames that only
 * contain an episode number (e.g. "Ep 07.mkv" in a `Season 3/` folder) still
 * resolve correctly. Returns a season number or null.
 */
function seasonFromParentDirs(absPath, rootDir) {
  const rel = path.relative(rootDir, absPath);
  const parts = rel.split(/[\\/]+/).slice(0, -1); // drop the file itself
  for (let i = parts.length - 1; i >= 0; i--) {
    const seg = parts[i];
    const m = /^(?:season|saison|s)[\s._-]?(\d{1,3})\b/i.exec(seg);
    if (m) return Number(m[1]);
  }
  return null;
}

/**
 * Walk a directory recursively, returning file metadata for every video file.
 * Skips hidden dirs (`.git`, `.DS_Store`, etc.) and anything starting with `_`.
 */
function walkVideoFiles(rootDir) {
  const out = [];
  const stack = [rootDir];
  while (stack.length) {
    const cur = stack.pop();
    let entries;
    try { entries = fs.readdirSync(cur, { withFileTypes: true }); }
    catch { continue; }
    for (const entry of entries) {
      if (entry.name.startsWith('.') || entry.name.startsWith('_')) continue;
      const p = path.join(cur, entry.name);
      if (entry.isDirectory()) { stack.push(p); continue; }
      if (!entry.isFile() || !VIDEO_EXT_RE.test(entry.name)) continue;
      let stat; try { stat = fs.statSync(p); } catch { continue; }
      out.push({ abs: p, rel: path.relative(rootDir, p).split(path.sep).join('/'), bytes: stat.size });
    }
  }
  out.sort((a, b) => a.rel.localeCompare(b.rel, undefined, { numeric: true }));
  return out;
}

/**
 * Try to extract a trailing/inline episode number from a filename that
 * doesn't match SxxExx. Used for absolute-episode decoding in title-scope
 * mode (v2).
 */
function extractBareEpisodeNumber(name) {
  const base = path.basename(name);
  // A number immediately before the extension: "Naruto_207.mkv", "Naruto - 207.mkv".
  let m = /[-_.\s](\d{2,4})(?=\.[a-z0-9]+$)/i.exec(base);
  if (m) return Number(m[1]);
  // "Ep 207" / "Episode 207" / "E207"
  m = /\b(?:episode|ep|e)[\s._-]?(\d{1,4})\b/i.exec(base);
  if (m) return Number(m[1]);
  return null;
}

/**
 * Match mode A — against pending requests (v1; used when no --title is passed).
 * Returns a match descriptor or a reason-to-skip.
 */
function matchFileToRequest(file, rootDir, pendingRequests) {
  let parsed = parseEpisodeFromFilename(file.rel);
  const parentSeason = seasonFromParentDirs(file.abs, rootDir);
  // If the filename gave us S=1 but the parent dir says otherwise, trust the
  // parent dir — common for `Show/Season X/ep.mkv` layouts.
  if (parsed && parentSeason != null && parsed.season === 1 && parentSeason !== 1) {
    parsed = { season: parentSeason, episode: parsed.episode };
  }
  // Filename-only parse failed but parent has a season + name has an episode num.
  if (!parsed && parentSeason != null) {
    const epNum = extractBareEpisodeNumber(file.rel);
    if (epNum != null) parsed = { season: parentSeason, episode: epNum };
  }
  if (!parsed) return { file, matched: false, reason: 'no S##E## pattern in filename or parent dir' };

  const candidates = pendingRequests.filter((r) =>
    r.episodeSeason === parsed.season && r.episodeNumber === parsed.episode
  );
  if (candidates.length === 0) {
    return { file, matched: false, season: parsed.season, episode: parsed.episode,
      reason: `no pending request for S${parsed.season}E${parsed.episode}` };
  }
  const uniqueTitles = new Set(candidates.map((c) => c.titleId));
  if (uniqueTitles.size > 1) {
    return { file, matched: false, season: parsed.season, episode: parsed.episode,
      reason: `ambiguous — ${uniqueTitles.size} titles have pending S${parsed.season}E${parsed.episode}` };
  }
  const best = candidates.find((c) => c.episodeId) || candidates[0];
  return {
    file,
    matched: true,
    season: parsed.season,
    episode: parsed.episode,
    titleId: best.titleId,
    episodeId: best.episodeId,
    titleTitle: best.titleTitle,
    episodeName: best.episodeName,
  };
}

/**
 * Match mode B — against the full episode list of ONE title (v2). Used when
 * the admin passes `--title <titleId>`. Supports SxxExx, parent-dir-season,
 * AND absolute-episode decoding (e.g. "Naruto 207" → S9E13).
 *
 * `episodesList` is `[{id, season, episode, name, absoluteIndex}]` sorted by
 * (season, episode). `maxPerSeason` is computed once per folder scan —
 * episode numbers larger than that can only be absolute, never per-season.
 */
function matchFileToTitleEpisode(file, rootDir, title, episodesList, maxPerSeason) {
  const parsed = parseEpisodeFromFilename(file.rel);
  const parentSeason = seasonFromParentDirs(file.abs, rootDir);

  // Case 1: filename has SxxExx (or NxNN, Season X Episode Y). Strong signal.
  if (parsed) {
    let season = parsed.season;
    let episode = parsed.episode;
    if (parentSeason != null && season === 1 && parentSeason !== 1) season = parentSeason;
    const ep = episodesList.find((e) => e.season === season && e.episode === episode);
    if (ep) {
      return { file, matched: true, season, episode,
        titleId: title.id, episodeId: ep.id, titleTitle: title.title, episodeName: ep.name };
    }
    return { file, matched: false, season, episode,
      reason: `no S${season}E${episode} in ${title.title}'s catalogue` };
  }

  // Case 2: no SxxExx. Try to extract a bare number.
  const bare = extractBareEpisodeNumber(file.rel);
  if (bare == null) {
    return { file, matched: false, reason: 'no episode number found in filename' };
  }

  // Case 2a: parent dir gives a season → treat the bare number as per-season.
  if (parentSeason != null) {
    const ep = episodesList.find((e) => e.season === parentSeason && e.episode === bare);
    if (ep) {
      return { file, matched: true, season: parentSeason, episode: bare,
        titleId: title.id, episodeId: ep.id, titleTitle: title.title, episodeName: ep.name };
    }
    // fall through to absolute attempt
  }

  // Case 2b: absolute-episode decode. If the number exceeds the largest
  // per-season episode count, it can ONLY be absolute. If it's small enough
  // to be per-season, prefer season 1 episode N if a parent-dir match failed.
  if (bare > maxPerSeason) {
    const ep = episodesList.find((e) => e.absoluteIndex === bare);
    if (ep) {
      return { file, matched: true, season: ep.season, episode: ep.episode,
        titleId: title.id, episodeId: ep.id, titleTitle: title.title, episodeName: ep.name,
        note: `absolute #${bare}`,
      };
    }
    return { file, matched: false,
      reason: `absolute episode ${bare} beyond ${title.title}'s range (${episodesList.length} total)` };
  }

  // Small number, no parent dir season, no SxxExx — try absolute first, then S1EX.
  const absEp = episodesList.find((e) => e.absoluteIndex === bare);
  const s1Ep = episodesList.find((e) => e.season === 1 && e.episode === bare);
  const ep = absEp || s1Ep;
  if (ep) {
    return { file, matched: true, season: ep.season, episode: ep.episode,
      titleId: title.id, episodeId: ep.id, titleTitle: title.title, episodeName: ep.name,
      note: absEp ? `absolute #${bare}` : 'guessed S1',
    };
  }
  return { file, matched: false, reason: `episode ${bare} not in ${title.title}'s catalogue` };
}

function renderScanSummary(matches, rootDir, mode) {
  const ok = matches.filter((m) => m.matched);
  const skip = matches.filter((m) => !m.matched);
  console.log(`\nScanned ${rootDir}  (mode: ${mode})`);
  console.log(`  ${matches.length} video file(s)  ·  ${ok.length} matched  ·  ${skip.length} skipped\n`);
  if (ok.length) {
    console.log('MATCHED (will ingest on `ingest-folder`):');
    for (const m of ok) {
      const ep = `S${m.season}E${m.episode}`;
      const note = m.note ? ` [${m.note}]` : '';
      console.log(`  ✓ ${m.file.rel}  →  ${ep}${note}  ${m.titleTitle || ''}`);
    }
    console.log('');
  }
  if (skip.length) {
    console.log('SKIPPED:');
    for (const m of skip) {
      console.log(`  · ${m.file.rel}  —  ${m.reason}`);
    }
    console.log('');
  }
}

/**
 * Build the list of matches for the folder. When `titleId` is given we fetch
 * that title's full episode list and match against it (v2 mode — supports
 * absolute-episode decoding). Otherwise we match against the server's
 * pending-request list (v1 mode).
 */
async function buildFolderMatches(api, folder, titleId) {
  const abs = path.resolve(folder);
  if (!fs.existsSync(abs) || !fs.statSync(abs).isDirectory()) {
    throw new Error(`Not a directory: ${abs}`);
  }
  const files = walkVideoFiles(abs);
  if (files.length === 0) return { abs, mode: 'empty', matches: [] };

  if (titleId) {
    // v2: scope to a single title; can decode absolute-episode.
    let listRes;
    try { listRes = await api.listEpisodes(titleId); }
    catch (e) { throw new Error(`Could not fetch episodes for title ${titleId}: ${e.message}`); }
    const episodes = listRes.episodes || [];
    if (episodes.length === 0) {
      throw new Error(`Title ${titleId} has no episodes synced. Run "Sync from TMDB" on its title page first.`);
    }
    const title = { id: titleId, title: listRes.titleTitle || '(unknown)' };
    // Max episodes-per-season decides when a bare number is definitely absolute.
    const perSeasonCounts = new Map();
    for (const e of episodes) {
      perSeasonCounts.set(e.season, (perSeasonCounts.get(e.season) || 0) + 1);
    }
    const maxPerSeason = Math.max(...perSeasonCounts.values(), 0);
    const matches = files.map((f) => matchFileToTitleEpisode(f, abs, title, episodes, maxPerSeason));
    return { abs, mode: `title-scope (${title.title}, ${episodes.length} eps)`, matches };
  }

  // v1: match against pending requests.
  const r = await api.listRequests();
  const pending = r.requests || [];
  const matches = files.map((f) => matchFileToRequest(f, abs, pending));
  return { abs, mode: `pending-requests (${pending.length} open)`, matches };
}

async function cmdScan(api, folder, titleId) {
  if (!folder) {
    console.error('Missing folder. Usage: aurasync-worker scan <folder> [--title <uuid>]');
    process.exit(2);
  }
  let result;
  try { result = await buildFolderMatches(api, folder, titleId); }
  catch (e) { console.error(e.message); process.exit(2); }
  if (result.mode === 'empty') { console.log(`No video files found in ${result.abs}.`); return; }
  renderScanSummary(result.matches, result.abs, result.mode);
}

async function cmdIngestFolder(api, cfg, caps, folder, titleId) {
  if (!folder) {
    console.error('Missing folder. Usage: aurasync-worker ingest-folder <folder> [--title <uuid>]');
    process.exit(2);
  }
  // Register so the server sees us online.
  try { await api.register(caps); }
  catch (e) { console.error(`register failed: ${e.message}`); process.exit(3); }

  let result;
  try { result = await buildFolderMatches(api, folder, titleId); }
  catch (e) { console.error(e.message); process.exit(2); }
  if (result.mode === 'empty') { console.log(`No video files found in ${result.abs}.`); return; }
  renderScanSummary(result.matches, result.abs, result.mode);

  const ok = result.matches.filter((m) => m.matched);
  if (ok.length === 0) {
    console.log('Nothing to ingest — no files matched.');
    if (!titleId) console.log('(Without --title, only episodes with pending requests are matched. Pass --title <uuid> to ingest into a title directly.)');
    return;
  }

  // Serial queue. NVENC is 100% during a transcode; parallel would thrash.
  // Upload-within-a-file is already 12-way parallel (see runLocalIngestJob).
  console.log(`\n=== ingesting ${ok.length} file(s) serially ===\n`);
  let done = 0, failed = 0;
  const failures = [];
  for (const m of ok) {
    done++;
    const label = `[${done}/${ok.length}] S${m.season}E${m.episode} · ${m.file.rel}`;
    console.log(label);
    const stat = fs.statSync(m.file.abs);
    let job;
    try {
      job = await api.ingestLocal({
        titleId: m.titleId,
        episodeId: m.episodeId || null,
        filename: path.basename(m.file.abs),
        bytes: stat.size,
      });
    } catch (e) {
      failed++; failures.push({ file: m.file.rel, reason: 'ingest-local: ' + e.message });
      warn(`  ingest-local failed: ${e.message} — skipping`);
      continue;
    }
    try {
      await runLocalIngestJob(api, cfg, job, m.file.abs, caps);
      log(`  ✓ done`);
    } catch (e) {
      failed++; failures.push({ file: m.file.rel, reason: 'transcode: ' + e.message });
      warn(`  transcode failed: ${e.message} — skipping to next`);
      try { await api.fail(job.jobId, e.message, false); } catch { /* ignore */ }
    }
  }

  console.log(`\n=== folder ingest summary ===`);
  console.log(`  ${ok.length - failed} succeeded · ${failed} failed · ${result.matches.length - ok.length} skipped (no match)`);
  if (failures.length) {
    console.log('\nFailures:');
    for (const f of failures) console.log(`  · ${f.file}  —  ${f.reason}`);
  }
}

// ---------- local GUI (http://127.0.0.1:<port>) ----------

/**
 * Single embedded HTML page. Served only to localhost. No external assets,
 * no third-party JS, no access to the worker token — the page talks only to
 * this same process via /api/*, which proxies to the AuraSync server with
 * the token it already has.
 */
const GUI_HTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AuraSync Worker — __NAME__</title>
<style>
:root {
  --bg: #0c0e14;
  --panel: #151826;
  --panel-2: #1d2030;
  --panel-hover: #242838;
  --border: rgba(255,255,255,0.07);
  --border-strong: rgba(255,255,255,0.14);
  --text: #e8eaf2;
  --text-dim: #9aa0b0;
  --text-muted: #6d7287;
  --accent: #5b8cff;
  --accent2: #9769ff;
  --accent-glow: rgba(91,140,255,0.3);
  --ok: #3fd8a0;
  --warn: #ffb547;
  --err: #ff5d6e;
  --radius: 10px;
  --radius-lg: 14px;
  --transition: 160ms cubic-bezier(.2,.8,.2,1);
}
* { box-sizing: border-box; }
html, body { margin: 0; height: 100%; background: var(--bg); color: var(--text); }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'SF Pro Display', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
  font-size: 14px; line-height: 1.45;
  -webkit-font-smoothing: antialiased;
  display: flex; flex-direction: column;
  background:
    radial-gradient(ellipse 700px 500px at 20% -10%, rgba(91,140,255,0.08), transparent 60%),
    radial-gradient(ellipse 700px 500px at 90% 110%, rgba(151,105,255,0.06), transparent 60%),
    var(--bg);
}
button { font-family: inherit; }

header.top {
  display: flex; align-items: center; gap: 14px;
  padding: 14px 22px;
  border-bottom: 1px solid var(--border);
}
header.top .brand {
  display: inline-flex; align-items: center; gap: 10px;
  font-size: 16px; font-weight: 700;
  background: linear-gradient(135deg, var(--accent), var(--accent2));
  -webkit-background-clip: text; background-clip: text;
  color: transparent;
}
header.top .brand-mark {
  width: 28px; height: 28px; border-radius: 8px;
  background: linear-gradient(135deg, var(--accent), var(--accent2));
  display: grid; place-items: center;
  color: #0a0a1a; font-size: 13px; font-weight: 900;
  -webkit-text-fill-color: #0a0a1a;
  box-shadow: 0 4px 14px var(--accent-glow);
}
header.top .meta { font-size: 12px; color: var(--text-dim); }
header.top .spacer { flex: 1; }
.pill {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 4px 10px; border-radius: 100px;
  font-size: 11px; font-weight: 600;
  text-transform: uppercase; letter-spacing: 0.06em;
}
.pill.ok { background: rgba(63,216,160,0.14); color: var(--ok); }
.pill.ok::before { content: ''; width: 6px; height: 6px; border-radius: 50%; background: var(--ok); box-shadow: 0 0 6px var(--ok); }
.pill.warn { background: rgba(255,181,71,0.14); color: var(--warn); }
.pill.err { background: rgba(255,93,110,0.14); color: var(--err); }

main {
  display: grid; grid-template-columns: 1fr 1fr; gap: 16px;
  padding: 18px 22px;
  flex: 1; min-height: 0;
}
@media (max-width: 900px) { main { grid-template-columns: 1fr; } }
section.pane {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  overflow: hidden;
  display: flex; flex-direction: column;
  min-height: 0;
}

.pane-header {
  padding: 12px 16px;
  border-bottom: 1px solid var(--border);
  display: flex; align-items: center; gap: 10px;
  font-size: 11px; font-weight: 600;
  color: var(--text-dim);
  text-transform: uppercase; letter-spacing: 0.08em;
}
.pane-header .count {
  font-size: 12px; color: var(--text); font-weight: 700;
  text-transform: none; letter-spacing: 0;
  padding: 1px 8px; background: var(--panel-2);
  border: 1px solid var(--border); border-radius: 100px;
}
.pane-header .spacer { flex: 1; }
.pane-header button, .pane-header input[type=text] {
  background: var(--panel-2); color: var(--text-dim);
  border: 1px solid var(--border);
  padding: 5px 10px; border-radius: 6px;
  font-size: 12px; cursor: pointer;
  transition: color var(--transition), border-color var(--transition);
}
.pane-header button:hover { color: var(--text); border-color: var(--border-strong); }
.pane-header input[type=text]:focus { outline: none; color: var(--text); border-color: var(--accent); }

.right-tabs {
  display: flex; gap: 2px; padding: 6px 10px 0;
  border-bottom: 1px solid var(--border);
}
.right-tab {
  background: transparent; border: none; border-bottom: 2px solid transparent;
  color: var(--text-dim);
  padding: 10px 18px; margin-bottom: -1px;
  font-size: 13px; font-weight: 600; cursor: pointer;
  transition: color var(--transition), border-color var(--transition);
}
.right-tab:hover { color: var(--text); }
.right-tab.active { color: var(--text); border-bottom-color: var(--accent); }
.right-tab-panel { display: flex; flex-direction: column; min-height: 0; flex: 1; }
.right-tab-panel[hidden] { display: none; }

.list { overflow-y: auto; flex: 1; min-height: 0; }
.row {
  padding: 12px 16px;
  border-bottom: 1px solid var(--border);
  cursor: pointer;
  transition: background var(--transition);
}
.row:hover { background: var(--panel-hover); }
.row.selected {
  background: linear-gradient(90deg, rgba(91,140,255,0.14), transparent);
  border-left: 3px solid var(--accent);
  padding-left: 13px;
}
.row .title { font-weight: 600; font-size: 14px; color: var(--text); }
.row .sub { font-size: 12px; color: var(--text-dim); margin-top: 3px; }
.row .meta-line { font-size: 11px; color: var(--text-muted); margin-top: 3px; font-family: ui-monospace, monospace; }
.row .bytes { font-size: 11px; color: var(--text-muted); margin-top: 2px; font-family: ui-monospace, monospace; }
.row.empty {
  padding: 40px 16px; text-align: center;
  color: var(--text-muted); cursor: default;
}
.row.empty:hover { background: transparent; }

.inbox-config {
  padding: 10px 16px;
  border-bottom: 1px solid var(--border);
  display: flex; align-items: center; gap: 8px;
  font-size: 12px; color: var(--text-muted);
}
.inbox-config input {
  flex: 1; background: var(--panel-2); color: var(--text);
  border: 1px solid var(--border); padding: 6px 10px; border-radius: 6px;
  font-family: ui-monospace, monospace; font-size: 12px;
}
.inbox-config button {
  background: var(--panel-2); color: var(--text-dim);
  border: 1px solid var(--border); padding: 6px 12px; border-radius: 6px;
  font-size: 12px; cursor: pointer;
}

.folder-controls {
  padding: 14px 16px;
  display: flex; flex-direction: column; gap: 10px;
  border-bottom: 1px solid var(--border);
}
.folder-controls label {
  font-size: 10px; color: var(--text-muted);
  text-transform: uppercase; letter-spacing: 0.08em;
  font-weight: 600;
}
.folder-controls input, .folder-controls select {
  background: var(--panel-2); color: var(--text);
  border: 1px solid var(--border);
  padding: 9px 12px; border-radius: 8px;
  font-size: 13px; font-family: ui-monospace, monospace;
}
.folder-controls select { font-family: inherit; }
.folder-controls input:focus, .folder-controls select:focus {
  outline: none; border-color: var(--accent);
  box-shadow: 0 0 0 3px var(--accent-glow);
}
.folder-actions { display: flex; gap: 8px; margin-top: 4px; }
.btn {
  padding: 9px 16px; border: 1px solid var(--border);
  background: var(--panel-2); color: var(--text);
  border-radius: 8px; cursor: pointer; font-size: 13px; font-weight: 500;
  transition: all var(--transition);
}
.btn:hover { border-color: var(--border-strong); background: var(--panel-hover); }
.btn.primary {
  background: linear-gradient(135deg, var(--accent), var(--accent2));
  color: #0a0a1a; border: none; font-weight: 700;
  box-shadow: 0 4px 14px var(--accent-glow);
}
.btn.primary:hover { transform: translateY(-1px); box-shadow: 0 6px 20px var(--accent-glow); }
.btn.ghost { background: transparent; color: var(--text-dim); border-color: var(--border); }
.btn:disabled { opacity: 0.45; cursor: not-allowed; transform: none !important; box-shadow: none !important; }

.folder-summary {
  padding: 10px 16px;
  border-bottom: 1px solid var(--border);
  font-size: 12px; color: var(--text-dim);
}
.folder-summary:empty { display: none; }
.folder-summary .tag {
  display: inline-block; padding: 2px 8px; border-radius: 100px;
  font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em;
  margin-right: 6px;
}
.folder-summary .tag.ok { background: rgba(63,216,160,0.15); color: var(--ok); }
.folder-summary .tag.warn { background: rgba(255,93,110,0.15); color: var(--err); }

#folder-results .row { cursor: default; }
#folder-results .row.matched .file-line { color: var(--text); }
#folder-results .row.skipped .file-line { color: var(--text-muted); opacity: 0.65; }
#folder-results .file-line { font-family: ui-monospace, monospace; font-size: 12px; }
#folder-results .meta-line .tag {
  padding: 2px 7px; border-radius: 100px;
  font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em;
  margin-right: 6px;
}
#folder-results .row.matched .meta-line .tag { background: rgba(63,216,160,0.15); color: var(--ok); }
#folder-results .row.skipped .meta-line .tag { background: rgba(109,114,135,0.15); color: var(--text-muted); }

.action-bar {
  padding: 12px 22px;
  border-top: 1px solid var(--border);
  background: var(--panel);
  display: flex; align-items: center; gap: 16px;
}
.action-bar .picked { flex: 1; display: flex; flex-direction: column; gap: 4px; min-width: 0; }
.action-bar .picked > div { font-size: 13px; display: flex; gap: 8px; align-items: center; min-width: 0; }
.action-bar .picked .lbl {
  font-size: 10px; color: var(--text-muted);
  text-transform: uppercase; letter-spacing: 0.08em;
  font-weight: 600; flex-shrink: 0; width: 66px;
}
.action-bar .picked .val { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.action-bar .picked .val.none { color: var(--text-muted); }

.activity {
  border-top: 1px solid var(--border);
  background: linear-gradient(180deg, var(--panel), var(--bg));
  padding: 14px 22px;
  transition: padding var(--transition);
}
.activity.expanded { padding: 18px 22px 22px; }
.activity-idle {
  display: flex; align-items: center; gap: 10px;
  font-size: 12px; color: var(--text-dim);
}
.activity-idle::before {
  content: ''; width: 8px; height: 8px; border-radius: 50%;
  background: var(--ok); box-shadow: 0 0 8px var(--ok);
}
.activity.expanded .activity-idle { display: none; }
.activity-expanded { display: none; }
.activity.expanded .activity-expanded { display: block; }

.act-head {
  display: flex; align-items: center; gap: 12px; margin-bottom: 10px;
}
.act-head .icon {
  width: 36px; height: 36px; border-radius: 10px;
  display: grid; place-items: center; font-size: 16px;
  background: var(--panel-2); color: var(--accent);
  flex-shrink: 0;
  transition: all var(--transition);
}
.act-head .icon.running {
  background: linear-gradient(135deg, rgba(91,140,255,0.25), rgba(151,105,255,0.25));
  color: var(--accent);
  animation: pulse-glow 1600ms ease-in-out infinite;
}
.act-head .icon.uploading { color: var(--ok); background: rgba(63,216,160,0.15); }
.act-head .icon.done { color: var(--ok); background: rgba(63,216,160,0.2); }
.act-head .icon.error { color: var(--err); background: rgba(255,93,110,0.15); }
@keyframes pulse-glow {
  0%, 100% { box-shadow: 0 0 0 0 rgba(91,140,255,0.4); }
  50% { box-shadow: 0 0 0 6px rgba(91,140,255,0); }
}
.act-head .info { flex: 1; min-width: 0; }
.act-head .info .title-row { font-size: 14px; font-weight: 600; color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; display: flex; align-items: center; gap: 8px; }
.act-head .info .phase-chip {
  padding: 2px 8px; border-radius: 100px; font-size: 10px; font-weight: 700;
  text-transform: uppercase; letter-spacing: 0.06em;
}
.act-head .info .phase-chip.transcoding { background: rgba(91,140,255,0.18); color: var(--accent); }
.act-head .info .phase-chip.uploading { background: rgba(63,216,160,0.18); color: var(--ok); }
.act-head .info .phase-chip.done { background: rgba(63,216,160,0.22); color: var(--ok); }
.act-head .info .phase-chip.error { background: rgba(255,93,110,0.18); color: var(--err); }
.act-head .info .sub { font-size: 12px; color: var(--text-dim); margin-top: 3px; font-family: ui-monospace, monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.act-head .cancel { flex-shrink: 0; }

.bar {
  height: 8px; background: var(--panel-2); border-radius: 100px; overflow: hidden;
  margin-bottom: 6px;
  box-shadow: inset 0 1px 2px rgba(0,0,0,0.3);
}
.bar > span {
  display: block; height: 100%;
  background: linear-gradient(90deg, var(--accent), var(--accent2));
  transition: width 300ms ease-out;
  border-radius: 100px;
}
.bar.uploading > span { background: linear-gradient(90deg, var(--ok), #70e8bc); }
.bar.done > span { background: var(--ok); }
.bar.error > span { background: var(--err); }
.bar-legend {
  display: flex; justify-content: space-between; align-items: center;
  font-size: 11px; color: var(--text-muted);
}
.bar-legend strong { color: var(--text); font-variant-numeric: tabular-nums; }
.bar-legend .right { display: flex; gap: 10px; font-variant-numeric: tabular-nums; }

.queue-section {
  margin-top: 14px; padding-top: 14px;
  border-top: 1px solid var(--border);
}
.queue-section .label {
  font-size: 10px; color: var(--text-muted);
  text-transform: uppercase; letter-spacing: 0.08em;
  font-weight: 600; margin-bottom: 6px;
}
.queue-section .bar > span {
  background: linear-gradient(90deg, var(--accent2), var(--accent));
}
.queue-legend {
  display: flex; justify-content: space-between;
  font-size: 11px; color: var(--text-muted); margin-top: 4px;
}
.queue-legend .right { display: flex; gap: 12px; font-variant-numeric: tabular-nums; }
.queue-legend .ok-c { color: var(--ok); }
.queue-legend .fail-c { color: var(--err); }

.history-head {
  display: flex; justify-content: space-between; align-items: center;
  margin-top: 14px; margin-bottom: 6px;
  font-size: 10px; color: var(--text-muted);
  text-transform: uppercase; letter-spacing: 0.08em;
  font-weight: 600;
}
.history-list {
  max-height: 180px; overflow-y: auto;
  background: var(--bg); border: 1px solid var(--border); border-radius: 8px;
}
.history-list:empty::after {
  content: 'No files completed yet.'; display: block;
  padding: 14px; text-align: center; color: var(--text-muted); font-size: 12px;
}
.history-item {
  display: flex; align-items: center; gap: 10px;
  padding: 8px 12px;
  font-size: 12px;
}
.history-item + .history-item { border-top: 1px solid var(--border); }
.history-item .mark {
  width: 18px; height: 18px; border-radius: 5px;
  display: grid; place-items: center;
  font-size: 10px; font-weight: 800;
  flex-shrink: 0;
}
.history-item.done .mark { background: rgba(63,216,160,0.18); color: var(--ok); }
.history-item.failed .mark { background: rgba(255,93,110,0.18); color: var(--err); }
.history-item .label-ep {
  font-size: 10px; font-weight: 700;
  padding: 2px 6px; border-radius: 100px;
  background: var(--panel-2); color: var(--text-dim);
  flex-shrink: 0; min-width: 44px; text-align: center;
}
.history-item .file { flex: 1; font-family: ui-monospace, monospace; color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; }
.history-item .dur { color: var(--text-muted); font-size: 11px; flex-shrink: 0; font-variant-numeric: tabular-nums; }
.history-item.failed .file { color: var(--text-dim); }

.hidden { display: none !important; }
.muted { color: var(--text-muted); }
</style>
</head>
<body>

<header class="top">
  <span class="brand"><span class="brand-mark">◉</span> AuraSync</span>
  <span class="meta">__NAME__ · __SERVER__</span>
  <span class="spacer"></span>
  <span class="pill ok" id="status-pill">idle</span>
</header>

<main>
  <section class="pane">
    <div class="pane-header">
      <span>Pending requests</span>
      <span class="count" id="req-count">–</span>
      <span class="spacer"></span>
      <button id="refresh-req">Refresh</button>
    </div>
    <div class="list" id="requests"></div>
  </section>

  <section class="pane">
    <div class="right-tabs" role="tablist">
      <button class="right-tab active" data-tab="files" role="tab">Files</button>
      <button class="right-tab" data-tab="folder" role="tab">Folder</button>
    </div>

    <div id="tab-files" class="right-tab-panel">
      <div class="pane-header">
        <span>Local files</span>
        <span class="count" id="file-count">–</span>
        <span class="spacer"></span>
        <input type="text" id="filter" placeholder="filter filenames…">
        <button id="refresh-files">Refresh</button>
      </div>
      <div class="inbox-config">
        Inbox:
        <input id="inbox-path" value="">
        <button id="save-inbox">Save</button>
      </div>
      <div class="list" id="files"></div>
    </div>

    <div id="tab-folder" class="right-tab-panel" hidden>
      <div class="folder-controls">
        <label>Folder path</label>
        <input id="folder-path" placeholder="C:\\TV\\The Big Bang Theory" />
        <label>Title scope (optional)</label>
        <select id="folder-title"><option value="">— any pending request —</option></select>
        <div class="folder-actions">
          <button id="folder-scan" class="btn">Scan</button>
          <button id="folder-ingest" class="btn primary" disabled>Ingest matched →</button>
        </div>
      </div>
      <div class="folder-summary" id="folder-summary"></div>
      <div class="list" id="folder-results"></div>
    </div>
  </section>
</main>

<div class="action-bar" id="action-bar">
  <div class="picked">
    <div><span class="lbl">Request</span><span id="picked-req" class="val none">none</span></div>
    <div><span class="lbl">File</span><span id="picked-file" class="val none">none</span></div>
  </div>
  <button class="btn primary" id="fulfill" disabled>Fulfill →</button>
</div>

<!-- Activity panel: always visible; collapses to a one-line 'Ready' strip
     when no ingest is active, expands to show current-file progress,
     queue progress, and recent history when something is running or
     just finished. Replaces the old modal progress overlay. -->
<div class="activity" id="activity">
  <div class="activity-idle">Ready — pick a request + file (Files tab) or paste a folder path (Folder tab) to start.</div>
  <div class="activity-expanded">
    <div class="act-head">
      <div class="icon" id="act-icon">⚙</div>
      <div class="info">
        <div class="title-row">
          <span id="act-title">—</span>
          <span class="phase-chip" id="act-phase-chip">starting</span>
        </div>
        <div class="sub" id="act-sub">—</div>
      </div>
      <button class="btn ghost cancel" id="act-cancel" type="button">Cancel</button>
    </div>

    <div class="bar" id="act-file-bar"><span id="act-file-fill" style="width:0%"></span></div>
    <div class="bar-legend">
      <span class="left"><strong id="act-file-pct">0%</strong> current file</span>
      <span class="right">
        <span id="act-file-elapsed">0:00</span>
        <span class="muted">elapsed</span>
      </span>
    </div>

    <div class="queue-section" id="act-queue-section" hidden>
      <div class="label">Queue progress</div>
      <div class="bar" id="act-queue-bar"><span id="act-queue-fill" style="width:0%"></span></div>
      <div class="queue-legend">
        <span><strong id="act-queue-n">0 / 0</strong> files</span>
        <span class="right">
          <span class="ok-c" id="act-queue-ok">0 done</span>
          <span class="fail-c" id="act-queue-fail">0 failed</span>
          <span id="act-queue-eta" class="muted">eta —</span>
        </span>
      </div>
    </div>

    <div class="history-head">
      <span>Recent</span>
      <span class="muted" id="act-history-count">0 items</span>
    </div>
    <div class="history-list" id="act-history"></div>
  </div>
</div>

<script>
(function(){
  const state = {
    requests: [],
    files: [],
    inbox: '',
    pickedReq: null,
    pickedFile: null,
    ingestRunning: false,
    // Folder-ingest panel state
    folder: {
      path: '',
      titleId: '',
      scanning: false,
      matches: [],
      mode: '',
      ingesting: false,
    },
    activeRightTab: 'files',  // 'files' | 'folder'
  };

  const $ = (id) => document.getElementById(id);
  const esc = (s) => { const d = document.createElement('div'); d.textContent = s == null ? '' : String(s); return d.innerHTML; };
  const fmtMB = (n) => (n/1024/1024).toFixed(1) + ' MB';

  async function api(path, opts) {
    const res = await fetch(path, opts);
    const text = await res.text();
    let json; try { json = text ? JSON.parse(text) : null; } catch {}
    if (!res.ok) throw new Error((json && json.error) || text || ('HTTP ' + res.status));
    return json;
  }

  async function loadRequests() {
    $('req-count').textContent = '…';
    try {
      const r = await api('/api/requests');
      state.requests = r.requests || [];
      $('req-count').textContent = state.requests.length;
      renderRequests();
    } catch (e) {
      $('req-count').textContent = '!';
      $('requests').innerHTML = '<div class="empty">Failed: ' + esc(e.message) + '</div>';
    }
  }

  function renderRequests() {
    const host = $('requests');
    if (state.requests.length === 0) {
      host.innerHTML = '<div class="empty">No pending requests.</div>';
      return;
    }
    host.innerHTML = state.requests.map((r, i) => {
      const ep = r.episodeSeason ? ' — S' + r.episodeSeason + 'E' + r.episodeNumber + (r.episodeName ? ' · ' + esc(r.episodeName) : '') : '';
      const year = r.titleYear ? ' (' + r.titleYear + ')' : '';
      const selected = state.pickedReq && state.pickedReq.requestId === r.requestId ? ' selected' : '';
      return '<div class="row' + selected + '" data-idx="' + i + '">' +
        '<div class="title">' + esc(r.titleTitle) + year + '<span class="muted"> · ' + esc(r.titleTmdbType || '?') + '</span>' + esc(ep) + '</div>' +
        '<div class="sub">by ' + esc(r.requesterName || '?') + ' · ' + new Date(r.createdAt).toLocaleString() + '</div>' +
      '</div>';
    }).join('');
    host.querySelectorAll('.row').forEach((el) => {
      el.addEventListener('click', () => {
        const idx = Number(el.dataset.idx);
        state.pickedReq = state.requests[idx];
        renderRequests();
        updateFooter();
      });
    });
  }

  async function loadFiles() {
    $('file-count').textContent = '…';
    try {
      const r = await api('/api/files');
      state.files = r.files || [];
      state.inbox = r.inbox || '';
      $('inbox-path').value = state.inbox;
      $('file-count').textContent = state.files.length;
      renderFiles();
    } catch (e) {
      $('file-count').textContent = '!';
      $('files').innerHTML = '<div class="empty">Failed: ' + esc(e.message) + '</div>';
    }
  }

  function renderFiles() {
    const host = $('files');
    const filter = $('filter').value.trim().toLowerCase();
    const shown = filter ? state.files.filter((f) => f.rel.toLowerCase().includes(filter)) : state.files;
    if (shown.length === 0) {
      host.innerHTML = '<div class="empty">' + (state.files.length === 0
        ? 'No video files in inbox. Drop .mp4/.mkv/.mov/.webm/.m4v/.avi into the inbox folder and refresh.'
        : 'No files match "' + esc(filter) + '".') + '</div>';
      return;
    }
    host.innerHTML = shown.map((f, i) => {
      const selected = state.pickedFile && state.pickedFile.abs === f.abs ? ' selected' : '';
      return '<div class="row' + selected + '" data-i="' + i + '" data-abs="' + esc(f.abs) + '">' +
        '<div class="title">' + esc(f.rel) + '</div>' +
        '<div class="bytes">' + fmtMB(f.bytes) + '</div>' +
      '</div>';
    }).join('');
    host.querySelectorAll('.row').forEach((el) => {
      el.addEventListener('click', () => {
        const abs = el.dataset.abs;
        state.pickedFile = state.files.find((f) => f.abs === abs);
        renderFiles();
        updateFooter();
      });
    });
  }

  function updateFooter() {
    const pr = state.pickedReq;
    const pf = state.pickedFile;
    const reqEl = $('picked-req');
    const fileEl = $('picked-file');
    if (pr) {
      const ep = pr.episodeSeason ? ' — S' + pr.episodeSeason + 'E' + pr.episodeNumber : '';
      reqEl.textContent = pr.titleTitle + ep;
      reqEl.className = 'val';
    } else {
      reqEl.textContent = 'none';
      reqEl.className = 'val none';
    }
    if (pf) { fileEl.textContent = pf.rel; fileEl.className = 'val'; }
    else { fileEl.textContent = 'none'; fileEl.className = 'val none'; }
    $('fulfill').disabled = !(pr && pf) || state.ingestRunning;
  }

  async function doFulfill() {
    if (!state.pickedReq || !state.pickedFile) return;
    const req = state.pickedReq;
    const file = state.pickedFile;
    state.ingestRunning = true;
    updateFooter();
    try {
      await api('/api/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          titleId: req.titleId,
          episodeId: req.episodeId || null,
          filePath: file.abs,
        }),
      });
    } catch (e) {
      showActivityError(e.message);
      state.ingestRunning = false;
      updateFooter();
      return;
    }
    pollStatus();
  }

  // ---- Activity panel (inline; replaces the old modal progress overlay) ----
  function fmtDur(ms) {
    if (ms == null || ms < 0) return '—';
    const s = Math.max(0, Math.floor(ms / 1000));
    const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
    if (h > 0) return h + ':' + String(m).padStart(2, '0') + ':' + String(sec).padStart(2, '0');
    return m + ':' + String(sec).padStart(2, '0');
  }
  function iconForPhase(phase) {
    if (phase === 'done') return '✓';
    if (phase === 'error') return '✕';
    if (phase === 'uploading') return '↑';
    if (phase === 'transcoding') return '▶';
    return '⚙';
  }
  function classForPhase(phase) {
    if (phase === 'done') return 'done';
    if (phase === 'error') return 'error';
    if (phase === 'uploading') return 'uploading';
    if (phase === 'transcoding') return 'running';
    return 'running';
  }

  function renderActivity(s) {
    const act = $('activity');
    const nowIdle = !s || (!s.active && !state.ingestRunning);
    act.classList.toggle('expanded', !nowIdle);

    if (nowIdle) return;

    // Determine effective phase — prefer currentFile.phase for fine detail,
    // else top-level phase.
    const phase = (s.currentFile && s.currentFile.phase) || s.phase || 'starting';
    const pclass = classForPhase(phase);
    $('act-icon').className = 'icon ' + pclass;
    $('act-icon').textContent = iconForPhase(phase);

    // Title row
    let titleText;
    if (s.queue) {
      const idx = Math.min(s.queue.currentIndex + 1, s.queue.total);
      titleText = 'Folder ingest · ' + idx + '/' + s.queue.total;
    } else {
      titleText = s.titleName || 'Transcoding';
    }
    $('act-title').textContent = titleText;

    const chip = $('act-phase-chip');
    chip.className = 'phase-chip ' + pclass;
    chip.textContent = phase === 'error' ? 'failed' : phase;

    // Sub line — current file path + encoder
    const cf = s.currentFile;
    const subParts = [];
    if (cf && cf.rel) subParts.push(cf.rel);
    else if (s.filePath) subParts.push(s.filePath);
    if (cf && cf.season != null) subParts.push('S' + cf.season + 'E' + cf.episode);
    if (s.encoder) subParts.push(s.encoder);
    $('act-sub').textContent = subParts.join(' · ') || '—';

    // Per-file bar
    const filePct = cf ? (cf.progress || 0) : (s.progress || 0);
    const pctInt = Math.round(filePct * 100);
    $('act-file-fill').style.width = pctInt + '%';
    $('act-file-fill').parentElement.className = 'bar ' + (phase === 'uploading' ? 'uploading' : phase === 'done' ? 'done' : phase === 'error' ? 'error' : '');
    $('act-file-pct').textContent = pctInt + '%';
    // File elapsed
    const fileStart = cf && cf.startedAt ? cf.startedAt : null;
    $('act-file-elapsed').textContent = fileStart ? fmtDur(Date.now() - fileStart) : '—';

    // Queue bar (folder ingest only)
    const q = s.queue;
    if (q) {
      $('act-queue-section').hidden = false;
      const qPct = q.total > 0 ? Math.round((q.done / q.total) * 100) : 0;
      $('act-queue-fill').style.width = qPct + '%';
      $('act-queue-n').textContent = q.done + ' / ' + q.total;
      $('act-queue-ok').textContent = (q.succeeded || 0) + ' done';
      $('act-queue-fail').textContent = (q.failed || 0) + ' failed';
      $('act-queue-eta').textContent = s.etaMs ? 'eta ' + fmtDur(s.etaMs) : 'eta —';
    } else {
      $('act-queue-section').hidden = true;
    }

    // History
    const hist = s.history || [];
    $('act-history-count').textContent = hist.length + ' items';
    $('act-history').innerHTML = hist.map((h) => {
      const cls = h.status === 'done' ? 'done' : 'failed';
      const mark = h.status === 'done' ? '✓' : '✕';
      const ep = h.episode ? '<span class="label-ep">' + esc(h.episode) + '</span>' : '';
      const dur = fmtDur(h.durationMs);
      const reasonLine = (h.status === 'failed' && h.reason)
        ? '<div class="reason">' + esc(h.reason) + '</div>'
        : '';
      return '<div class="history-item ' + cls + '">' +
        '<span class="mark">' + mark + '</span>' +
        ep +
        '<div style="flex:1;min-width:0">' +
          '<div class="file">' + esc(h.rel) + '</div>' +
          reasonLine +
        '</div>' +
        '<span class="dur">' + dur + '</span>' +
      '</div>';
    }).join('');

    // Cancel button shown while actively running, hidden on done/error.
    $('act-cancel').style.display = (phase === 'done' || phase === 'error') ? 'none' : '';
  }

  function showActivityError(msg) {
    const act = $('activity');
    act.classList.add('expanded');
    $('act-icon').className = 'icon error';
    $('act-icon').textContent = '✕';
    $('act-title').textContent = 'Failed';
    const chip = $('act-phase-chip');
    chip.className = 'phase-chip error';
    chip.textContent = 'failed';
    $('act-sub').textContent = msg || '';
    $('act-file-fill').style.width = '0%';
    $('act-file-fill').parentElement.className = 'bar error';
    $('act-cancel').style.display = 'none';
  }

  let pollTimer = null;
  async function pollStatus() {
    if (pollTimer) clearTimeout(pollTimer);
    try {
      const s = await api('/api/state');
      renderActivity(s);
      if (s.active) {
        if (s.phase === 'done') {
          state.ingestRunning = false;
          setTimeout(() => {
            loadRequests();
            state.pickedReq = null; state.pickedFile = null;
            updateFooter();
          }, 1500);
          // Keep polling briefly so the panel shows the final state; then stop.
          pollTimer = setTimeout(pollStatus, 3000);
        } else if (s.phase === 'error') {
          state.ingestRunning = false;
          updateFooter();
        } else {
          pollTimer = setTimeout(pollStatus, 500);  // faster polling for live progress
        }
      } else {
        pollTimer = setTimeout(pollStatus, 1000);
      }
    } catch {
      pollTimer = setTimeout(pollStatus, 2000);
    }
  }

  async function saveInbox() {
    const p = $('inbox-path').value.trim();
    if (!p) return;
    try {
      await api('/api/inbox', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ inbox: p }) });
      await loadFiles();
    } catch (e) { alert('Save failed: ' + e.message); }
  }

  $('refresh-req').onclick = loadRequests;
  $('refresh-files').onclick = loadFiles;
  $('filter').addEventListener('input', renderFiles);
  $('save-inbox').onclick = saveInbox;
  $('fulfill').onclick = doFulfill;
  $('act-cancel').onclick = async () => {
    if (!confirm('Cancel the current transcode?')) return;
    try { await api('/api/cancel', { method: 'POST' }); } catch {}
  };

  // ---- Right-column tab toggle + Folder-ingest panel ----
  document.querySelectorAll('.right-tab').forEach((tab) => {
    tab.addEventListener('click', () => {
      const name = tab.dataset.tab;
      state.activeRightTab = name;
      document.querySelectorAll('.right-tab').forEach((t) =>
        t.classList.toggle('active', t.dataset.tab === name));
      $('tab-files').hidden = name !== 'files';
      $('tab-folder').hidden = name !== 'folder';
      if (name === 'folder' && state.requests.length === 0) {
        // surface pending-request titles in the scope dropdown lazily
        void loadRequests();
      }
      if (name === 'folder') populateTitleScope();
    });
  });

  function populateTitleScope() {
    const sel = $('folder-title');
    const curVal = sel.value;
    const seen = new Map();
    for (const r of state.requests) {
      if (r.titleId && !seen.has(r.titleId)) seen.set(r.titleId, r.titleTitle || r.titleId.slice(0, 8));
    }
    sel.innerHTML = '<option value="">— any pending request —</option>' +
      Array.from(seen.entries()).map(([id, name]) =>
        '<option value="' + esc(id) + '">' + esc(name) + '</option>'
      ).join('');
    if (curVal && seen.has(curVal)) sel.value = curVal;
  }

  async function folderScan() {
    const p = $('folder-path').value.trim();
    if (!p) return;
    state.folder.path = p;
    state.folder.titleId = $('folder-title').value || '';
    state.folder.scanning = true;
    $('folder-scan').disabled = true;
    $('folder-ingest').disabled = true;
    $('folder-summary').innerHTML = 'Scanning…';
    $('folder-results').innerHTML = '';
    try {
      const r = await api('/api/folder-scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ folder: p, titleId: state.folder.titleId || null }),
      });
      state.folder.matches = r.matches || [];
      state.folder.mode = r.mode || '';
      renderFolderResults();
    } catch (e) {
      $('folder-summary').innerHTML = '<span class="warn">' + esc(e.message) + '</span>';
      state.folder.matches = [];
    } finally {
      state.folder.scanning = false;
      $('folder-scan').disabled = false;
    }
  }

  function renderFolderResults() {
    const matches = state.folder.matches;
    const ok = matches.filter((m) => m.matched);
    const skip = matches.filter((m) => !m.matched);
    $('folder-summary').innerHTML =
      '<span class="ok">' + ok.length + ' match' + (ok.length === 1 ? '' : 'es') + '</span>' +
      ' · <span class="warn">' + skip.length + ' skipped</span>' +
      ' · mode: ' + esc(state.folder.mode);
    $('folder-ingest').disabled = ok.length === 0 || state.folder.ingesting;
    if (matches.length === 0) {
      $('folder-results').innerHTML = '<div class="empty">No video files.</div>';
      return;
    }
    // Show matched first, then skipped
    const rows = [...ok, ...skip];
    $('folder-results').innerHTML = rows.map((m) => {
      const cls = m.matched ? 'matched' : 'skipped';
      const tag = m.matched
        ? '<span class="tag">S' + m.season + 'E' + m.episode + (m.note ? ' ' + esc(m.note) : '') + '</span>'
        : '<span class="tag">skip</span>';
      const meta = m.matched
        ? (esc(m.titleTitle || '') + (m.episodeName ? ' · ' + esc(m.episodeName) : ''))
        : esc(m.reason || '');
      return '<div class="row ' + cls + '">' +
        '<div class="file">' + esc(m.file.rel) + '</div>' +
        '<div class="meta">' + tag + meta + '</div>' +
        '</div>';
    }).join('');
  }

  async function folderIngest() {
    const ok = state.folder.matches.filter((m) => m.matched);
    if (ok.length === 0) return;
    if (!confirm('Ingest ' + ok.length + ' file' + (ok.length === 1 ? '' : 's') + ' serially? Transcode will run one at a time.')) return;
    state.folder.ingesting = true;
    state.ingestRunning = true;
    $('folder-ingest').disabled = true;
    // Kick off, then let the poll + activity panel do the rest.
    try {
      await api('/api/folder-ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ folder: state.folder.path, titleId: state.folder.titleId || null }),
      });
      pollStatus();
    } catch (e) {
      showActivityError(e.message);
      state.folder.ingesting = false;
      state.ingestRunning = false;
    }
  }

  $('folder-scan').onclick = folderScan;
  $('folder-ingest').onclick = folderIngest;
  $('folder-title').addEventListener('change', () => { state.folder.titleId = $('folder-title').value || ''; });

  // Initial load + polling.
  loadRequests();
  loadFiles();
  setInterval(() => { if (!state.ingestRunning) loadRequests(); }, 15_000);
})();
</script>
</body>
</html>`;

// In-memory state for the single active ingest. Only one runs at a time; the
// UI enforces this, and runtime enforces too via ingestState.active.
const ingestState = {
  active: false,
  titleId: null,
  episodeId: null,
  titleName: null,
  filePath: null,
  jobId: null,
  progress: 0,           // overall queue progress 0..1
  phase: 'idle',         // 'idle' | 'starting' | 'transcoding' | 'uploading' | 'done' | 'error'
  error: null,
  encoder: null,
  startedAt: null,
  childFfmpeg: null,     // ref for cancel
  // Per-file detail (same shape whether it's a single-file ingest or the
  // current item of a folder queue). Lets the GUI show file-level %
  // separately from queue-level %.
  currentFile: null,     // { rel, bytes, startedAt, progress (0..1), phase }
  // Queue tracking (for folder ingests; single-file ingest leaves these null).
  queue: null,           // { total, done, succeeded, failed, currentIndex }
  // Rolling history of recent files across this session so the Activity panel
  // can show a log — newest first. Capped at HISTORY_MAX entries.
  history: [],           // [{ rel, status: 'done'|'failed', durationMs, bytes, episode, reason? }]
};
const HISTORY_MAX = 30;

function pushHistory(entry) {
  ingestState.history.unshift(entry);
  if (ingestState.history.length > HISTORY_MAX) {
    ingestState.history.length = HISTORY_MAX;
  }
}

function readJsonBody(req, limit = 64 * 1024) {
  return new Promise((resolve, reject) => {
    let total = 0;
    const chunks = [];
    req.on('data', (c) => {
      total += c.length;
      if (total > limit) { req.destroy(); reject(new Error('body too large')); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {}); }
      catch (err) { reject(err); }
    });
    req.on('error', reject);
  });
}

function sendJson(res, status, body) {
  const s = JSON.stringify(body);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': Buffer.byteLength(s) });
  res.end(s);
}

const VIDEO_RE = /\.(mp4|mkv|mov|webm|m4v|avi|ts|flv|wmv)$/i;
function listInboxFiles(inbox) {
  const out = [];
  if (!inbox || !fs.existsSync(inbox)) return out;
  const root = path.resolve(inbox);
  const stack = [root];
  while (stack.length) {
    const cur = stack.pop();
    let entries;
    try { entries = fs.readdirSync(cur, { withFileTypes: true }); } catch { continue; }
    for (const e of entries) {
      const p = path.join(cur, e.name);
      if (e.isDirectory()) { stack.push(p); continue; }
      if (!e.isFile() || !VIDEO_RE.test(e.name)) continue;
      let stat; try { stat = fs.statSync(p); } catch { continue; }
      out.push({ abs: p, rel: path.relative(root, p).split(path.sep).join('/'), bytes: stat.size });
    }
  }
  out.sort((a, b) => a.rel.localeCompare(b.rel));
  return out;
}

async function runGuiIngestJob(api, cfg, caps, filePath, titleId, episodeId, titleName) {
  const startedAt = Date.now();
  ingestState.active = true;
  ingestState.titleId = titleId;
  ingestState.episodeId = episodeId || null;
  ingestState.titleName = titleName || '';
  ingestState.filePath = filePath;
  ingestState.progress = 0;
  ingestState.phase = 'starting';
  ingestState.error = null;
  ingestState.encoder = caps.picked;
  ingestState.startedAt = startedAt;
  ingestState.queue = null;  // single-file: no queue
  ingestState.currentFile = {
    rel: path.basename(filePath),
    bytes: 0, progress: 0, phase: 'starting', startedAt,
  };
  const rel = path.basename(filePath);
  try {
    const stat = fs.statSync(filePath);
    ingestState.currentFile.bytes = stat.size;
    const job = await api.ingestLocal({
      titleId, episodeId: episodeId || null,
      filename: path.basename(filePath), bytes: stat.size,
    });
    ingestState.jobId = job.jobId;
    ingestState.phase = 'transcoding';
    ingestState.currentFile.phase = 'transcoding';

    const workDir = path.join(cfg.tmpDir, job.jobId);
    fs.mkdirSync(workDir, { recursive: true });
    const hlsDir = path.join(workDir, 'hls');
    fs.mkdirSync(hlsDir, { recursive: true });
    for (const v of ['v1080p']) fs.mkdirSync(path.join(hlsDir, v), { recursive: true });

    const durationSec = ffprobeDurationSec(cfg, filePath);
    const args = buildFfmpegArgs(cfg, filePath, hlsDir, caps);
    await runFfmpegWithProgress(cfg, args, durationSec || 0, (pct) => {
      const p = Math.max(0, Math.min(0.9, pct * 0.90));
      ingestState.progress = p;
      if (ingestState.currentFile) ingestState.currentFile.progress = p;
      api.progress(job.jobId, p).catch(() => {});
    }, (child) => { ingestState.childFfmpeg = child; });

    ingestState.phase = 'uploading';
    if (ingestState.currentFile) ingestState.currentFile.phase = 'uploading';
    const { totalBytes } = await uploadHlsTree(api, job.jobId, hlsDir, (done, total) => {
      const p = Math.min(0.99, 0.90 + (done / total) * 0.10);
      ingestState.progress = p;
      if (ingestState.currentFile) ingestState.currentFile.progress = p;
      api.progress(job.jobId, p).catch(() => {});
    });

    await api.complete(job.jobId, { durationSec, bytes: totalBytes });
    try { fs.rmSync(workDir, { recursive: true, force: true }); } catch { /* ignore */ }
    ingestState.progress = 1;
    ingestState.phase = 'done';
    if (ingestState.currentFile) ingestState.currentFile.progress = 1;
    pushHistory({ rel, status: 'done', durationMs: Date.now() - startedAt, bytes: totalBytes, episode: null });
  } catch (err) {
    const reason = err.message || String(err);
    ingestState.phase = 'error';
    ingestState.error = reason;
    if (ingestState.jobId) {
      try { await api.fail(ingestState.jobId, reason, false); } catch { /* ignore */ }
    }
    pushHistory({ rel, status: 'failed', durationMs: Date.now() - startedAt, bytes: 0, episode: null, reason });
  } finally {
    ingestState.active = false;
    ingestState.childFfmpeg = null;
  }
}

/**
 * Folder-ingest driver for the GUI. Scans the folder, then runs matched
 * files through runLocalIngestJob serially. Updates ingestState between
 * each file so the GUI's progress overlay shows overall queue progress
 * (N/M files) alongside the current file's transcode + upload %.
 */
async function runGuiFolderIngest(api, cfg, caps, folder, titleId) {
  let result;
  try { result = await buildFolderMatches(api, folder, titleId); }
  catch (e) {
    ingestState.active = true;
    ingestState.phase = 'error';
    ingestState.error = e.message;
    ingestState.active = false;
    return;
  }
  const matches = (result.matches || []).filter((m) => m.matched);
  if (matches.length === 0) {
    ingestState.active = true;
    ingestState.phase = 'error';
    ingestState.error = 'No matched files in folder — nothing to ingest.';
    ingestState.active = false;
    return;
  }

  const queueStart = Date.now();
  ingestState.active = true;
  ingestState.titleId = null;
  ingestState.episodeId = null;
  ingestState.titleName = `Folder · ${matches.length} file${matches.length === 1 ? '' : 's'}`;
  ingestState.filePath = folder;
  ingestState.progress = 0;
  ingestState.phase = 'starting';
  ingestState.error = null;
  ingestState.encoder = caps.picked;
  ingestState.startedAt = queueStart;
  ingestState.queue = { total: matches.length, done: 0, succeeded: 0, failed: 0, currentIndex: 0 };

  let succeeded = 0;
  let failed = 0;
  for (let i = 0; i < matches.length; i++) {
    const m = matches[i];
    const stat = fs.statSync(m.file.abs);
    const fileStart = Date.now();
    ingestState.phase = `transcoding`;
    ingestState.titleId = m.titleId;
    ingestState.episodeId = m.episodeId || null;
    ingestState.filePath = m.file.abs;
    ingestState.queue.currentIndex = i;
    ingestState.currentFile = {
      rel: m.file.rel, bytes: stat.size, startedAt: fileStart,
      progress: 0, phase: 'starting',
      season: m.season, episode: m.episode,
      episodeName: m.episodeName || null, titleTitle: m.titleTitle || null,
    };

    let job;
    try {
      job = await api.ingestLocal({
        titleId: m.titleId,
        episodeId: m.episodeId || null,
        filename: path.basename(m.file.abs),
        bytes: stat.size,
      });
      ingestState.jobId = job.jobId;
    } catch (e) {
      failed++;
      ingestState.queue.failed = failed;
      warn(`folder-ingest: ingest-local failed for ${m.file.rel}: ${e.message}`);
      pushHistory({ rel: m.file.rel, status: 'failed', durationMs: Date.now() - fileStart, bytes: 0,
        episode: `S${m.season}E${m.episode}`, reason: 'ingest-local: ' + e.message });
      continue;
    }

    try {
      const workDir = path.join(cfg.tmpDir, job.jobId);
      fs.mkdirSync(workDir, { recursive: true });
      const hlsDir = path.join(workDir, 'hls');
      fs.mkdirSync(hlsDir, { recursive: true });
      for (const v of ['v1080p']) fs.mkdirSync(path.join(hlsDir, v), { recursive: true });

      const durationSec = ffprobeDurationSec(cfg, m.file.abs);
      const args = buildFfmpegArgs(cfg, m.file.abs, hlsDir, caps);
      const fileBase = (i / matches.length);
      const fileShare = 1 / matches.length;
      ingestState.currentFile.phase = 'transcoding';
      await runFfmpegWithProgress(cfg, args, durationSec || 0, (pct) => {
        const filePct = Math.max(0, Math.min(0.9, pct * 0.90));
        ingestState.currentFile.progress = filePct;
        ingestState.progress = fileBase + filePct * fileShare;
        api.progress(job.jobId, filePct).catch(() => {});
      }, (child) => { ingestState.childFfmpeg = child; });

      ingestState.currentFile.phase = 'uploading';
      const { totalBytes } = await uploadHlsTree(api, job.jobId, hlsDir, (done, total) => {
        const filePct = 0.90 + (done / total) * 0.10;
        ingestState.currentFile.progress = filePct;
        ingestState.progress = fileBase + filePct * fileShare;
        api.progress(job.jobId, Math.min(0.99, filePct)).catch(() => {});
      });
      await api.complete(job.jobId, { durationSec, bytes: totalBytes });
      try { fs.rmSync(workDir, { recursive: true, force: true }); } catch { /* ignore */ }
      succeeded++;
      ingestState.queue.succeeded = succeeded;
      pushHistory({ rel: m.file.rel, status: 'done', durationMs: Date.now() - fileStart, bytes: totalBytes,
        episode: `S${m.season}E${m.episode}` });
    } catch (e) {
      failed++;
      ingestState.queue.failed = failed;
      warn(`folder-ingest: transcode failed for ${m.file.rel}: ${e.message}`);
      try { await api.fail(job.jobId, e.message, false); } catch { /* ignore */ }
      pushHistory({ rel: m.file.rel, status: 'failed', durationMs: Date.now() - fileStart, bytes: 0,
        episode: `S${m.season}E${m.episode}`, reason: 'transcode: ' + e.message });
    }
    ingestState.queue.done = i + 1;
    ingestState.progress = (i + 1) / matches.length;
  }

  ingestState.progress = 1;
  ingestState.phase = 'done';
  ingestState.titleName = `Folder · ${succeeded} succeeded, ${failed} failed`;
  ingestState.active = false;
  ingestState.childFfmpeg = null;
}

async function cmdGui(api, cfg, caps) {
  const args = parseArgs();
  const port = args.port || cfg.port || 4849;
  const bind = '127.0.0.1';
  // Inbox: CLI > config > default.
  let inbox = args.inbox || cfg.inbox || path.join(os.homedir(), 'aurasync-inbox');
  // Persist inbox override to config so subsequent runs remember.
  try {
    const disk = fs.existsSync(CONFIG_PATH) ? JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')) : {};
    disk.inbox = inbox;
    disk.port = port;
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(disk, null, 2));
  } catch { /* ignore */ }
  try { fs.mkdirSync(inbox, { recursive: true }); } catch { /* ignore */ }

  // Register with the server so it sees us as online while the GUI runs.
  try { await api.register(caps); } catch (e) { err(`register failed: ${e.message}`); process.exit(3); }
  const hb = setInterval(() => { api.heartbeat(caps).catch(() => {}); }, 30_000).unref();

  const http = require('http');
  const server = http.createServer(async (req, res) => {
    // Reject non-localhost requests.
    const host = (req.headers.host || '').split(':')[0];
    if (host !== '127.0.0.1' && host !== 'localhost') {
      res.writeHead(403); res.end('forbidden'); return;
    }
    try {
      if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
        const html = GUI_HTML
          .replace(/__NAME__/g, escapeHtml(cfg.name))
          .replace(/__SERVER__/g, escapeHtml(cfg.server));
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(html);
        return;
      }
      if (req.method === 'GET' && req.url === '/api/requests') {
        const r = await api.listRequests();
        sendJson(res, 200, r);
        return;
      }
      if (req.method === 'GET' && req.url === '/api/files') {
        sendJson(res, 200, { inbox, files: listInboxFiles(inbox) });
        return;
      }
      if (req.method === 'POST' && req.url === '/api/inbox') {
        const body = await readJsonBody(req);
        const next = String(body.inbox || '').trim();
        if (!next) { sendJson(res, 400, { error: 'missing inbox' }); return; }
        inbox = path.resolve(next);
        try { fs.mkdirSync(inbox, { recursive: true }); } catch {}
        try {
          const disk = fs.existsSync(CONFIG_PATH) ? JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')) : {};
          disk.inbox = inbox; fs.writeFileSync(CONFIG_PATH, JSON.stringify(disk, null, 2));
        } catch {}
        sendJson(res, 200, { inbox });
        return;
      }
      if (req.method === 'POST' && req.url === '/api/ingest') {
        if (ingestState.active) { sendJson(res, 409, { error: 'another ingest is already running' }); return; }
        const body = await readJsonBody(req);
        const titleId = String(body.titleId || '');
        const episodeId = body.episodeId ? String(body.episodeId) : null;
        const filePath = String(body.filePath || '');
        if (!titleId || !filePath) { sendJson(res, 400, { error: 'titleId and filePath required' }); return; }
        if (!fs.existsSync(filePath)) { sendJson(res, 404, { error: 'file not found on worker' }); return; }
        // Look up the title name for display (best-effort).
        let titleName = '';
        try {
          const rs = await api.listRequests();
          const hit = (rs.requests || []).find((r) => r.titleId === titleId);
          if (hit) titleName = hit.titleTitle || '';
        } catch { /* ignore */ }
        // Kick off in background.
        runGuiIngestJob(api, cfg, caps, filePath, titleId, episodeId, titleName);
        sendJson(res, 202, { accepted: true });
        return;
      }
      if (req.method === 'GET' && req.url === '/api/state') {
        // ETA: based on queue pace so far. For single-file ingest, simple
        // linear extrapolation from progress + elapsed.
        const now = Date.now();
        const startedAt = ingestState.startedAt || now;
        const elapsedMs = Math.max(0, now - startedAt);
        let etaMs = null;
        if (ingestState.active && ingestState.progress > 0.02 && ingestState.progress < 1) {
          etaMs = Math.round(elapsedMs * (1 - ingestState.progress) / ingestState.progress);
        }
        sendJson(res, 200, {
          active: ingestState.active || (ingestState.phase === 'done' || ingestState.phase === 'error'),
          phase: ingestState.phase,
          progress: ingestState.progress,
          error: ingestState.error,
          titleName: ingestState.titleName,
          filePath: ingestState.filePath,
          encoder: ingestState.encoder,
          jobId: ingestState.jobId,
          currentFile: ingestState.currentFile,
          queue: ingestState.queue,
          history: ingestState.history,
          elapsedMs,
          etaMs,
        });
        return;
      }
      if (req.method === 'POST' && req.url === '/api/cancel') {
        // Best-effort: kill ffmpeg if alive, which makes runFfmpegWithProgress
        // reject and the ingest loop reports failure to the server.
        try { if (ingestState.childFfmpeg) ingestState.childFfmpeg.kill('SIGKILL'); } catch {}
        sendJson(res, 200, { ok: true });
        return;
      }
      if (req.method === 'POST' && req.url === '/api/folder-scan') {
        const body = await readJsonBody(req);
        const folder = String(body.folder || '').trim();
        const scopeTitleId = body.titleId ? String(body.titleId) : null;
        if (!folder) { sendJson(res, 400, { error: 'missing folder' }); return; }
        try {
          const result = await buildFolderMatches(api, folder, scopeTitleId);
          sendJson(res, 200, {
            abs: result.abs,
            mode: result.mode,
            matches: result.matches,
          });
        } catch (e) {
          sendJson(res, 400, { error: e.message });
        }
        return;
      }
      if (req.method === 'POST' && req.url === '/api/folder-ingest') {
        if (ingestState.active) { sendJson(res, 409, { error: 'another ingest is already running' }); return; }
        const body = await readJsonBody(req);
        const folder = String(body.folder || '').trim();
        const scopeTitleId = body.titleId ? String(body.titleId) : null;
        if (!folder) { sendJson(res, 400, { error: 'missing folder' }); return; }
        // Kick off the queue in the background; ingestState is updated as
        // each file runs so /api/state shows progress the same way single-
        // file ingests do. The client polls /api/state until phase === 'done'.
        runGuiFolderIngest(api, cfg, caps, folder, scopeTitleId)
          .catch((e) => { ingestState.phase = 'error'; ingestState.error = e.message; ingestState.active = false; });
        sendJson(res, 202, { accepted: true });
        return;
      }
      res.writeHead(404); res.end('not found');
    } catch (e) {
      sendJson(res, 500, { error: e.message });
    }
  });
  server.listen(port, bind, () => {
    const url = `http://${bind}:${port}/`;
    console.log(`\n🎛  GUI ready — open in your browser:`);
    console.log(`   ${url}\n`);
    console.log(`  inbox: ${inbox}`);
    console.log(`  (drop .mp4 / .mkv / .mov / .webm / .m4v / .avi files into the inbox, then click Refresh in the GUI.)\n`);
    // Try to auto-open the browser (best-effort; swallow spawn errors so
    // missing `xdg-open` on headless Linux doesn't crash the process).
    const openCmd =
      process.platform === 'win32' ? ['cmd', ['/c', 'start', '""', url]] :
      process.platform === 'darwin' ? ['open', [url]] :
      ['xdg-open', [url]];
    try {
      const c = spawn(openCmd[0], openCmd[1], { stdio: 'ignore', detached: true });
      c.on('error', () => { /* no browser launcher available; user opens it manually */ });
      c.unref();
    } catch { /* ignore */ }
  });
  process.on('SIGINT', () => { server.close(); clearInterval(hb); process.exit(0); });
  process.on('SIGTERM', () => { server.close(); clearInterval(hb); process.exit(0); });
}

function escapeHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// ---------- main loop ----------

let shuttingDown = false;
process.on('SIGINT', () => { log('SIGINT — finishing current job then exiting'); shuttingDown = true; });
process.on('SIGTERM', () => { log('SIGTERM — finishing current job then exiting'); shuttingDown = true; });

async function main() {
  const cfg = loadConfig();
  const args = parseArgs();
  const sub = args.positional[0] || 'run';

  console.log(`\n==== AuraSync worker "${cfg.name}" ====`);
  console.log(`   server: ${cfg.server}`);
  console.log(`   tmpDir: ${cfg.tmpDir}\n`);
  const caps = detectCapabilities(cfg);
  caps.name = cfg.name;
  console.log(`   ffmpeg: ${caps.ffmpegVersion}`);
  console.log(`  encoder: ${caps.picked}${caps.picked !== 'libx264' ? ' (hardware-accelerated)' : ' (software)'}`);
  console.log(`     cpus: ${caps.cpus}   mem: ${(caps.memoryMB / 1024).toFixed(1)} GB\n`);

  fs.mkdirSync(cfg.tmpDir, { recursive: true });

  const api = makeApi(cfg);

  // ----- subcommand dispatch -----
  if (sub === 'requests' || sub === 'ls') {
    await cmdRequests(api);
    process.exit(0);
  }
  if (sub === 'ingest') {
    // positional[0]='ingest', positional[1]=<file>
    const filePath = args.positional[1];
    await cmdIngest(api, cfg, caps, filePath, args.titleId, args.episodeId);
    process.exit(0);
  }
  if (sub === 'scan') {
    const folder = args.positional[1];
    await cmdScan(api, folder, args.titleId);
    process.exit(0);
  }
  if (sub === 'ingest-folder') {
    const folder = args.positional[1];
    await cmdIngestFolder(api, cfg, caps, folder, args.titleId);
    process.exit(0);
  }
  if (sub === 'gui') {
    await cmdGui(api, cfg, caps);
    // Don't exit — the HTTP server keeps the process alive.
    return;
  }
  if (sub !== 'run') {
    console.error(`Unknown subcommand: ${sub}. Run with --help.`);
    process.exit(2);
  }
  // ----- default: poll/run mode -----

  // Register + heartbeat-every-30s regardless of job state.
  try {
    const reg = await api.register(caps);
    log(`registered as worker ${reg.worker.id.slice(0, 8)} ("${reg.worker.name}")`);
  } catch (e) {
    err(`register failed: ${e.message}`);
    process.exit(3);
  }

  const hb = setInterval(() => {
    api.heartbeat(caps).catch((e) => warn(`heartbeat failed: ${e.message}`));
  }, 30_000).unref();

  // Main claim loop.
  while (!shuttingDown) {
    let payload;
    try {
      payload = await api.claim(caps);
    } catch (e) {
      warn(`claim failed: ${e.message} — backing off 10s`);
      await new Promise((r) => setTimeout(r, 10_000));
      continue;
    }
    if (!payload || payload.empty) {
      // No job ready; the server's claim endpoint already held for ~25s.
      await new Promise((r) => setTimeout(r, 2000));
      continue;
    }
    log(`claimed job ${payload.jobId.slice(0, 8)} (title ${payload.titleId.slice(0, 8)}${payload.episodeId ? ', episode ' + payload.episodeId.slice(0, 8) : ''})`);
    try {
      await runOneJob(api, cfg, payload, caps);
    } catch (e) {
      err(`job ${payload.jobId.slice(0, 8)} failed: ${e.message}`);
      // Report failure so server requeues for another worker / local transcoder.
      try { await api.fail(payload.jobId, e.message, false); } catch (e2) { warn(`fail-report failed: ${e2.message}`); }
    }
  }
  clearInterval(hb);
  log('shutdown complete');
}

// Entry.
main().catch((e) => {
  err('fatal:', e.message);
  process.exit(1);
});
