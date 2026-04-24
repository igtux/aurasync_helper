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
    --bg: #0c0c14; --panel: #15151f; --border: #2a2a3a;
    --text: #e6e6f0; --muted: #8a8aa0; --accent: #7c5cff; --accent2: #4ec9ff;
    --ok: #00f5a0; --warn: #ffa502; --err: #ff4757;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px ui-sans-serif, system-ui, sans-serif; background: var(--bg); color: var(--text); }
  header {
    display: flex; align-items: center; gap: 12px; padding: 14px 20px;
    border-bottom: 1px solid var(--border); background: var(--panel);
  }
  header h1 { margin: 0; font-size: 16px; }
  header .spacer { flex: 1; }
  header .meta { font-size: 12px; color: var(--muted); }
  .pill { display: inline-block; padding: 3px 9px; border-radius: 999px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; }
  .pill.ok { background: rgba(0,245,160,0.2); color: var(--ok); }
  .pill.warn { background: rgba(255,165,2,0.2); color: var(--warn); }
  .pill.err { background: rgba(255,71,87,0.2); color: var(--err); }
  main { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; padding: 16px 20px; }
  @media (max-width: 800px) { main { grid-template-columns: 1fr; } }
  section { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }
  section h2 {
    margin: 0; padding: 12px 16px; font-size: 13px; text-transform: uppercase;
    letter-spacing: 0.06em; color: var(--muted); background: rgba(255,255,255,0.02);
    border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 8px;
  }
  section h2 .count { color: var(--text); font-weight: 600; }
  section h2 .spacer { flex: 1; }
  section h2 input[type="text"] {
    background: transparent; border: 1px solid var(--border); color: var(--text);
    padding: 4px 8px; border-radius: 6px; font-size: 12px; min-width: 120px;
  }
  section h2 button {
    background: transparent; color: var(--muted); border: 1px solid var(--border);
    padding: 3px 8px; border-radius: 6px; font-size: 11px; cursor: pointer;
  }
  section h2 button:hover { color: var(--text); border-color: var(--accent); }
  .list { max-height: 58vh; overflow-y: auto; }
  .row { padding: 10px 16px; border-bottom: 1px solid var(--border); cursor: pointer; }
  .row:hover { background: rgba(255,255,255,0.03); }
  .row.selected { background: rgba(124,92,255,0.15); border-left: 3px solid var(--accent); padding-left: 13px; }
  .row .title { font-weight: 600; }
  .row .sub { font-size: 12px; color: var(--muted); margin-top: 2px; }
  .row .muted { color: var(--muted); }
  .row .bytes { font-size: 11px; color: var(--muted); margin-top: 2px; font-family: ui-monospace, monospace; }
  .empty { padding: 20px; text-align: center; color: var(--muted); font-size: 13px; }
  footer {
    position: sticky; bottom: 0; background: var(--panel); border-top: 1px solid var(--border);
    padding: 14px 20px; display: flex; align-items: center; gap: 16px;
  }
  footer .picked { flex: 1; font-size: 13px; }
  footer .picked .lbl { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; margin-right: 6px; }
  button.primary {
    background: linear-gradient(90deg, var(--accent), var(--accent2)); color: #000;
    border: none; padding: 10px 20px; border-radius: 8px; font-weight: 600; cursor: pointer;
  }
  button.primary:disabled { opacity: 0.45; cursor: not-allowed; }
  .progress-overlay {
    position: fixed; inset: 0; background: rgba(0,0,0,0.8); display: none;
    align-items: center; justify-content: center; z-index: 100;
  }
  .progress-overlay.active { display: flex; }
  .progress-card {
    background: var(--panel); border: 1px solid var(--border); border-radius: 12px;
    padding: 24px; min-width: 360px; max-width: 480px;
  }
  .progress-card h3 { margin: 0 0 6px; }
  .progress-card .sub { color: var(--muted); font-size: 13px; margin-bottom: 14px; }
  .bar { height: 8px; background: rgba(255,255,255,0.08); border-radius: 999px; overflow: hidden; }
  .bar > span { display: block; height: 100%; width: 0%; background: linear-gradient(90deg, var(--accent), var(--accent2)); transition: width 0.3s; }
  .phase { margin-top: 8px; font-size: 12px; color: var(--muted); }
  .progress-card .err-msg { color: var(--err); margin-top: 10px; font-size: 13px; }
  .progress-card .ok-msg { color: var(--ok); margin-top: 10px; font-size: 13px; }
  .progress-card .actions { margin-top: 14px; display: flex; gap: 8px; justify-content: flex-end; }
  .progress-card .actions button { background: transparent; border: 1px solid var(--border); color: var(--text); padding: 6px 14px; border-radius: 6px; cursor: pointer; }
  .inbox-config {
    padding: 10px 16px; background: rgba(255,255,255,0.02); border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 8px; font-size: 12px; color: var(--muted);
  }
  .inbox-config code { font-family: ui-monospace, monospace; color: var(--text); background: rgba(255,255,255,0.04); padding: 2px 6px; border-radius: 4px; }
  .inbox-config input { flex: 1; background: var(--bg); color: var(--text); border: 1px solid var(--border); padding: 5px 8px; border-radius: 4px; font-family: ui-monospace, monospace; font-size: 12px; }

  /* Right-column tabs (Files / Folder) */
  .right-tabs {
    display: flex; gap: 4px; padding: 8px 12px 0;
    border-bottom: 1px solid var(--border); background: rgba(255,255,255,0.02);
  }
  .right-tab {
    background: transparent; border: 1px solid transparent; color: var(--muted);
    padding: 6px 14px; border-radius: 6px 6px 0 0; font-size: 12px;
    font-weight: 600; cursor: pointer; border-bottom: 2px solid transparent;
    margin-bottom: -1px;
  }
  .right-tab:hover { color: var(--text); }
  .right-tab.active {
    color: var(--text); border-bottom-color: var(--accent);
  }
  .right-tab-panel[hidden] { display: none; }

  /* Folder panel */
  .folder-controls {
    padding: 14px 16px; display: flex; flex-direction: column; gap: 8px;
    border-bottom: 1px solid var(--border); background: rgba(255,255,255,0.02);
  }
  .folder-controls label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; }
  .folder-controls input, .folder-controls select {
    background: var(--bg); color: var(--text); border: 1px solid var(--border);
    padding: 7px 10px; border-radius: 6px; font-size: 13px;
    font-family: ui-monospace, monospace;
  }
  .folder-controls select { font-family: inherit; }
  .folder-actions { display: flex; gap: 8px; margin-top: 4px; }
  .folder-actions button {
    padding: 7px 14px; border: 1px solid var(--border); background: var(--panel);
    color: var(--text); border-radius: 6px; cursor: pointer; font-size: 13px;
  }
  .folder-actions button.primary {
    background: linear-gradient(90deg, var(--accent), var(--accent2)); color: #000;
    border: none; font-weight: 600;
  }
  .folder-actions button:disabled { opacity: 0.5; cursor: not-allowed; }
  .folder-summary {
    padding: 10px 16px; font-size: 12px; color: var(--muted);
    border-bottom: 1px solid var(--border); background: rgba(255,255,255,0.02);
  }
  .folder-summary:empty { display: none; }
  .folder-summary .ok { color: var(--ok); }
  .folder-summary .warn { color: var(--warn); }
  #folder-results .row { display: block; }
  #folder-results .row .file { font-family: ui-monospace, monospace; font-size: 12px; }
  #folder-results .row.matched .file { color: var(--text); }
  #folder-results .row.skipped .file { color: var(--muted); text-decoration: line-through; opacity: 0.7; }
  #folder-results .row .meta { font-size: 11px; color: var(--muted); margin-top: 2px; }
  #folder-results .row.matched .meta .tag {
    background: rgba(0,245,160,0.15); color: var(--ok);
    padding: 1px 6px; border-radius: 999px; font-size: 10px; margin-right: 4px;
    text-transform: uppercase; letter-spacing: 0.05em;
  }
  #folder-results .row.skipped .meta .tag {
    background: rgba(138,138,160,0.15); color: var(--muted);
    padding: 1px 6px; border-radius: 999px; font-size: 10px; margin-right: 4px;
    text-transform: uppercase; letter-spacing: 0.05em;
  }
</style>
</head>
<body>

<header>
  <h1>AuraSync Worker</h1>
  <span class="meta">__NAME__ · __SERVER__</span>
  <span class="spacer"></span>
  <span class="pill ok" id="status-pill">idle</span>
</header>

<main>
  <section>
    <h2>
      <span>Pending requests</span>
      <span class="count" id="req-count">–</span>
      <span class="spacer"></span>
      <button id="refresh-req">Refresh</button>
    </h2>
    <div class="list" id="requests"></div>
  </section>

  <section>
    <div class="right-tabs" role="tablist">
      <button class="right-tab active" data-tab="files" role="tab">Files</button>
      <button class="right-tab" data-tab="folder" role="tab">Folder</button>
    </div>

    <div id="tab-files" class="right-tab-panel">
      <h2>
        <span>Local files</span>
        <span class="count" id="file-count">–</span>
        <span class="spacer"></span>
        <input type="text" id="filter" placeholder="filter filenames…">
        <button id="refresh-files">Refresh</button>
      </h2>
      <div class="inbox-config">
        Inbox:
        <input id="inbox-path" value="">
        <button id="save-inbox">Save</button>
      </div>
      <div class="list" id="files"></div>
    </div>

    <div id="tab-folder" class="right-tab-panel" hidden>
      <div class="folder-controls">
        <label>Folder</label>
        <input id="folder-path" placeholder="C:\\TV\\The Big Bang Theory" />
        <label>Title scope (optional)</label>
        <select id="folder-title"><option value="">— any pending request —</option></select>
        <div class="folder-actions">
          <button id="folder-scan">Scan</button>
          <button id="folder-ingest" class="primary" disabled>Ingest matched →</button>
        </div>
      </div>
      <div class="folder-summary" id="folder-summary"></div>
      <div class="list" id="folder-results"></div>
    </div>
  </section>
</main>

<footer>
  <div class="picked">
    <div><span class="lbl">Request</span><span id="picked-req">none</span></div>
    <div><span class="lbl">File</span><span id="picked-file">none</span></div>
  </div>
  <button class="primary" id="fulfill" disabled>Fulfill →</button>
</footer>

<div class="progress-overlay" id="progress-overlay">
  <div class="progress-card">
    <h3 id="p-title">Transcoding</h3>
    <div class="sub" id="p-sub">—</div>
    <div class="bar"><span id="p-bar"></span></div>
    <div class="phase" id="p-phase">starting…</div>
    <div class="err-msg" id="p-err" style="display:none"></div>
    <div class="ok-msg" id="p-ok" style="display:none"></div>
    <div class="actions">
      <button id="p-close" style="display:none">Close</button>
      <button id="p-cancel">Cancel</button>
    </div>
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
    if (pr) {
      const ep = pr.episodeSeason ? ' — S' + pr.episodeSeason + 'E' + pr.episodeNumber : '';
      $('picked-req').textContent = pr.titleTitle + ep;
    } else {
      $('picked-req').textContent = 'none';
    }
    $('picked-file').textContent = pf ? pf.rel : 'none';
    $('fulfill').disabled = !(pr && pf) || state.ingestRunning;
  }

  async function doFulfill() {
    if (!state.pickedReq || !state.pickedFile) return;
    const req = state.pickedReq;
    const file = state.pickedFile;
    showProgress(true, req.titleTitle, file.rel);
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
      showError(e.message);
      state.ingestRunning = false;
      updateFooter();
      return;
    }
    pollStatus();
  }

  let pollTimer = null;
  async function pollStatus() {
    if (pollTimer) clearTimeout(pollTimer);
    try {
      const s = await api('/api/state');
      if (s.active) {
        updateProgress(s);
        if (s.phase === 'done') {
          showOk('Title is now available in the catalogue.');
          state.ingestRunning = false;
          // Refresh requests — the fulfilled one should have disappeared.
          setTimeout(() => { loadRequests(); state.pickedReq = null; state.pickedFile = null; updateFooter(); }, 1500);
        } else if (s.phase === 'error') {
          showError(s.error || 'transcode failed');
          state.ingestRunning = false;
          updateFooter();
        } else {
          pollTimer = setTimeout(pollStatus, 1000);
        }
      } else {
        pollTimer = setTimeout(pollStatus, 1000);
      }
    } catch {
      pollTimer = setTimeout(pollStatus, 2000);
    }
  }

  function showProgress(open, titleName, fileName) {
    $('progress-overlay').classList.toggle('active', !!open);
    $('p-title').textContent = 'Transcoding ' + titleName;
    $('p-sub').textContent = fileName;
    $('p-phase').textContent = 'starting…';
    $('p-bar').style.width = '0%';
    $('p-err').style.display = 'none';
    $('p-ok').style.display = 'none';
    $('p-close').style.display = 'none';
    $('p-cancel').style.display = '';
  }
  function updateProgress(s) {
    $('p-bar').style.width = (Math.round((s.progress || 0) * 100)) + '%';
    $('p-phase').textContent = s.phase + ' · ' + Math.round((s.progress || 0) * 100) + '%' + (s.encoder ? ' · ' + s.encoder : '');
  }
  function showError(msg) {
    $('p-err').textContent = 'Failed: ' + msg;
    $('p-err').style.display = '';
    $('p-close').style.display = '';
    $('p-cancel').style.display = 'none';
  }
  function showOk(msg) {
    $('p-ok').textContent = '✓ ' + msg;
    $('p-ok').style.display = '';
    $('p-close').style.display = '';
    $('p-cancel').style.display = 'none';
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
  $('p-close').onclick = () => { $('progress-overlay').classList.remove('active'); };
  $('p-cancel').onclick = async () => {
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
    showProgress(true, state.folder.path.split(/[\\/]+/).pop() || 'folder', ok.length + ' file(s)');
    try {
      await api('/api/folder-ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ folder: state.folder.path, titleId: state.folder.titleId || null }),
      });
      pollStatus();
    } catch (e) {
      showError(e.message);
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
  progress: 0,
  phase: 'idle',     // 'idle' | 'starting' | 'transcoding' | 'uploading' | 'done' | 'error'
  error: null,
  encoder: null,
  startedAt: null,
  childFfmpeg: null, // ref for cancel
};

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
  ingestState.active = true;
  ingestState.titleId = titleId;
  ingestState.episodeId = episodeId || null;
  ingestState.titleName = titleName || '';
  ingestState.filePath = filePath;
  ingestState.progress = 0;
  ingestState.phase = 'starting';
  ingestState.error = null;
  ingestState.encoder = caps.picked;
  ingestState.startedAt = Date.now();
  try {
    const stat = fs.statSync(filePath);
    const job = await api.ingestLocal({
      titleId, episodeId: episodeId || null,
      filename: path.basename(filePath), bytes: stat.size,
    });
    ingestState.jobId = job.jobId;
    ingestState.phase = 'transcoding';

    // Run the same local-ingest flow used by the CLI, but route progress
    // into ingestState for the GUI to poll. We can't reuse runLocalIngestJob
    // directly because we need finer-grained phase labels.
    const workDir = path.join(cfg.tmpDir, job.jobId);
    fs.mkdirSync(workDir, { recursive: true });
    const hlsDir = path.join(workDir, 'hls');
    fs.mkdirSync(hlsDir, { recursive: true });
    for (const v of ['v1080p']) fs.mkdirSync(path.join(hlsDir, v), { recursive: true });

    const durationSec = ffprobeDurationSec(cfg, filePath);
    const args = buildFfmpegArgs(cfg, filePath, hlsDir, caps);
    await runFfmpegWithProgress(cfg, args, durationSec || 0, (pct) => {
      ingestState.progress = Math.max(0, Math.min(0.9, pct * 0.90));
      api.progress(job.jobId, ingestState.progress).catch(() => {});
    }, (child) => { ingestState.childFfmpeg = child; });

    ingestState.phase = 'uploading';
    const { totalBytes } = await uploadHlsTree(api, job.jobId, hlsDir, (done, total) => {
      const pct = 0.90 + (done / total) * 0.10;
      ingestState.progress = Math.min(0.99, pct);
      api.progress(job.jobId, ingestState.progress).catch(() => {});
    });

    await api.complete(job.jobId, { durationSec, bytes: totalBytes });
    try { fs.rmSync(workDir, { recursive: true, force: true }); } catch { /* ignore */ }
    ingestState.progress = 1;
    ingestState.phase = 'done';
  } catch (err) {
    ingestState.phase = 'error';
    ingestState.error = err.message || String(err);
    if (ingestState.jobId) {
      try { await api.fail(ingestState.jobId, ingestState.error, false); } catch { /* ignore */ }
    }
  } finally {
    // Leave state in place so the GUI can show 'done' / 'error' briefly.
    // The next call to runGuiIngestJob resets it.
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

  ingestState.active = true;
  ingestState.titleId = null;
  ingestState.episodeId = null;
  ingestState.titleName = `Folder · ${matches.length} file${matches.length === 1 ? '' : 's'}`;
  ingestState.filePath = folder;
  ingestState.progress = 0;
  ingestState.phase = 'starting';
  ingestState.error = null;
  ingestState.encoder = caps.picked;
  ingestState.startedAt = Date.now();

  let succeeded = 0;
  let failed = 0;
  for (let i = 0; i < matches.length; i++) {
    const m = matches[i];
    const stat = fs.statSync(m.file.abs);
    ingestState.phase = `file ${i + 1}/${matches.length} · S${m.season}E${m.episode}`;
    ingestState.titleId = m.titleId;
    ingestState.episodeId = m.episodeId || null;
    ingestState.filePath = m.file.abs;

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
      warn(`folder-ingest: ingest-local failed for ${m.file.rel}: ${e.message}`);
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
      await runFfmpegWithProgress(cfg, args, durationSec || 0, (pct) => {
        // This file's ffmpeg contributes the first 90% of its share of
        // overall progress. Uploads fill in the last 10%.
        ingestState.progress = fileBase + pct * 0.90 * fileShare;
        api.progress(job.jobId, Math.max(0, Math.min(0.9, pct * 0.90))).catch(() => {});
      }, (child) => { ingestState.childFfmpeg = child; });

      const { totalBytes } = await uploadHlsTree(api, job.jobId, hlsDir, (done, total) => {
        const localPct = 0.90 + (done / total) * 0.10;
        ingestState.progress = fileBase + localPct * fileShare;
        api.progress(job.jobId, Math.min(0.99, localPct)).catch(() => {});
      });
      await api.complete(job.jobId, { durationSec, bytes: totalBytes });
      try { fs.rmSync(workDir, { recursive: true, force: true }); } catch { /* ignore */ }
      succeeded++;
    } catch (e) {
      failed++;
      warn(`folder-ingest: transcode failed for ${m.file.rel}: ${e.message}`);
      try { await api.fail(job.jobId, e.message, false); } catch { /* ignore */ }
    }
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
        sendJson(res, 200, {
          active: ingestState.active || (ingestState.phase === 'done' || ingestState.phase === 'error'),
          phase: ingestState.phase,
          progress: ingestState.progress,
          error: ingestState.error,
          titleName: ingestState.titleName,
          filePath: ingestState.filePath,
          encoder: ingestState.encoder,
          jobId: ingestState.jobId,
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
