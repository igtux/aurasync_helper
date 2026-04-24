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

// Shared tail: HLS muxer output. Same for every encoder branch below.
function hlsMuxerTail(outDir) {
  return [
    '-g', '48', '-keyint_min', '48',
    '-c:a', 'aac', '-ar', '48000',
    '-b:a:0', '96k', '-b:a:1', '128k', '-b:a:2', '128k',
    '-f', 'hls',
    '-hls_time', '6',
    '-hls_playlist_type', 'vod',
    '-hls_flags', 'independent_segments',
    '-hls_segment_type', 'mpegts',
    '-hls_segment_filename', path.join(outDir, 'v%v', 'seg_%05d.ts'),
    '-master_pl_name', 'master.m3u8',
    '-var_stream_map', 'v:0,a:0,name:480p v:1,a:1,name:720p v:2,a:2,name:1080p',
    path.join(outDir, 'v%v', 'index.m3u8'),
  ];
}

/**
 * Fully-GPU pipeline for NVENC: decode → split → 3× scale_cuda → encode.
 * Frames never leave VRAM. On a consumer GPU (RTX 3080 / 4070 class) this
 * runs 3-5× faster than the PCIe-bouncing path because:
 *
 *   - without `-hwaccel_output_format cuda`, ffmpeg copies each decoded
 *     frame to host RAM, does 3× CPU swscale, then uploads 3× to NVENC.
 *     CPU + PCIe are the bottleneck even though NVENC itself is idle.
 *
 *   - WITH cuda output format + scale_cuda, the surface never touches
 *     host memory. NVENC consumes CUDA frames directly.
 *
 * Requires ffmpeg built with both CUDA and the scale_cuda filter (Gyan's
 * Windows "essentials" + "full" builds both have it; the static Linux
 * builds shipped by johnvansickle do too).
 */
function buildNvencArgs(srcPath, outDir) {
  return [
    '-y', '-hide_banner', '-loglevel', 'error',
    '-progress', 'pipe:2', '-nostats',

    // GPU decode AND keep decoded surfaces in VRAM.
    '-hwaccel', 'cuda',
    '-hwaccel_output_format', 'cuda',

    '-i', srcPath,

    // One decode → fan out to 3 scalers on-GPU.
    '-filter_complex',
    '[0:v]split=3[v0][v1][v2];' +
    '[v0]scale_cuda=854:480[o0];' +
    '[v1]scale_cuda=1280:720[o1];' +
    '[v2]scale_cuda=1920:1080[o2]',

    '-map', '[o0]', '-map', '[o1]', '-map', '[o2]',
    '-map', '0:a:0?', '-map', '0:a:0?', '-map', '0:a:0?',

    '-c:v', 'h264_nvenc',
    '-preset', 'p4',           // p1 fastest .. p7 slowest. p4 balances well.
    '-rc', 'vbr',
    '-b:v:0', '800k',  '-maxrate:v:0', '856k',  '-bufsize:v:0', '1200k',
    '-b:v:1', '2500k', '-maxrate:v:1', '2675k', '-bufsize:v:1', '3750k',
    '-b:v:2', '5000k', '-maxrate:v:2', '5350k', '-bufsize:v:2', '7500k',

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

    '-filter_complex',
    '[0:v]split=3[v0][v1][v2];' +
    '[v0]scale_qsv=854:480[o0];' +
    '[v1]scale_qsv=1280:720[o1];' +
    '[v2]scale_qsv=1920:1080[o2]',

    '-map', '[o0]', '-map', '[o1]', '-map', '[o2]',
    '-map', '0:a:0?', '-map', '0:a:0?', '-map', '0:a:0?',

    '-c:v', 'h264_qsv',
    '-preset', 'veryfast',
    '-b:v:0', '800k',  '-maxrate:v:0', '856k',  '-bufsize:v:0', '1200k',
    '-b:v:1', '2500k', '-maxrate:v:1', '2675k', '-bufsize:v:1', '3750k',
    '-b:v:2', '5000k', '-maxrate:v:2', '5350k', '-bufsize:v:2', '7500k',

    ...hlsMuxerTail(outDir),
  ];
}

/**
 * Fallback software pipeline — used for libx264, h264_amf, h264_videotoolbox,
 * and anywhere the GPU-resident filters aren't available. Does CPU scaling
 * via swscale (-s:v:N); for h264_amf/h264_videotoolbox this still benefits
 * from GPU encoding, just loses the zero-copy scale win.
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
    '-map', '0:v:0', '-map', '0:v:0', '-map', '0:v:0',
    '-map', '0:a:0?', '-map', '0:a:0?', '-map', '0:a:0?',
    '-c:v', enc,
  ];
  if (preset) args.push('-preset', preset);
  if (isSw) args.push('-profile:v', 'main');
  args.push(
    '-pix_fmt', 'yuv420p',
    '-sc_threshold', '0',
    '-s:v:0', '854x480',   '-b:v:0', '800k',  '-maxrate:v:0', '856k',  '-bufsize:v:0', '1200k',
    '-s:v:1', '1280x720',  '-b:v:1', '2500k', '-maxrate:v:1', '2675k', '-bufsize:v:1', '3750k',
    '-s:v:2', '1920x1080', '-b:v:2', '5000k', '-maxrate:v:2', '5350k', '-bufsize:v:2', '7500k',
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

// ---------- one-job runner ----------

async function runOneJob(api, cfg, job, caps) {
  const jobId = job.jobId;
  const workDir = path.join(cfg.tmpDir, jobId);
  fs.mkdirSync(workDir, { recursive: true });
  const srcExt = (path.extname(new URL(job.source.url).pathname).replace(/[?#].*$/, '') || '.mp4').toLowerCase();
  const srcPath = path.join(workDir, 'src' + srcExt);
  const hlsDir = path.join(workDir, 'hls');
  fs.mkdirSync(hlsDir, { recursive: true });
  for (const v of ['v480p', 'v720p', 'v1080p']) {
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

  // 3. Upload everything under hlsDir → keyPrefix/<relative>
  const files = walkFiles(hlsDir);
  log(`uploading ${files.length} HLS files`);
  let totalBytes = 0;
  for (let i = 0; i < files.length; i++) {
    const abs = files[i];
    const rel = path.relative(hlsDir, abs).split(path.sep).join('/');
    const { bytes } = await uploadFile(api, jobId, abs, rel);
    totalBytes += bytes;
    const pct = 0.90 + ((i + 1) / files.length) * 0.10;
    api.progress(jobId, Math.min(0.99, pct)).catch(() => {});
  }

  // 4. Tell server we're done → it flips the title to available.
  await api.complete(jobId, { durationSec, bytes: totalBytes });

  // 5. Clean up temp files.
  try { fs.rmSync(workDir, { recursive: true, force: true }); } catch { /* ignore */ }

  log(`job ${jobId.slice(0, 8)} complete (${files.length} files, ${(totalBytes / 1024 / 1024).toFixed(1)} MB)`);
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
  for (const v of ['v480p', 'v720p', 'v1080p']) {
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

  // 3. Upload HLS output to R2.
  const files = walkFiles(hlsDir);
  log(`uploading ${files.length} HLS files`);
  let totalBytes = 0;
  for (let i = 0; i < files.length; i++) {
    const abs = files[i];
    const rel = path.relative(hlsDir, abs).split(path.sep).join('/');
    const { bytes } = await uploadFile(api, jobId, abs, rel);
    totalBytes += bytes;
    const pct = 0.90 + ((i + 1) / files.length) * 0.10;
    api.progress(jobId, Math.min(0.99, pct)).catch(() => {});
  }

  await api.complete(jobId, { durationSec, bytes: totalBytes });
  try { fs.rmSync(workDir, { recursive: true, force: true }); } catch { /* ignore */ }
  log(`local-ingest job ${jobId.slice(0, 8)} complete (${files.length} files, ${(totalBytes / 1024 / 1024).toFixed(1)} MB)`);
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
    for (const v of ['v480p', 'v720p', 'v1080p']) fs.mkdirSync(path.join(hlsDir, v), { recursive: true });

    const durationSec = ffprobeDurationSec(cfg, filePath);
    const args = buildFfmpegArgs(cfg, filePath, hlsDir, caps);
    await runFfmpegWithProgress(cfg, args, durationSec || 0, (pct) => {
      ingestState.progress = Math.max(0, Math.min(0.9, pct * 0.90));
      api.progress(job.jobId, ingestState.progress).catch(() => {});
    }, (child) => { ingestState.childFfmpeg = child; });

    ingestState.phase = 'uploading';
    const files = walkFiles(hlsDir);
    let totalBytes = 0;
    for (let i = 0; i < files.length; i++) {
      const abs = files[i];
      const rel = path.relative(hlsDir, abs).split(path.sep).join('/');
      const { bytes } = await uploadFile(api, job.jobId, abs, rel);
      totalBytes += bytes;
      const pct = 0.90 + ((i + 1) / files.length) * 0.10;
      ingestState.progress = Math.min(0.99, pct);
      api.progress(job.jobId, ingestState.progress).catch(() => {});
    }

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
