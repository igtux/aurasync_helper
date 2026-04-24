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
  };
  if (!merged.server || !merged.token) {
    console.error('Missing --server and/or --token. See --help.');
    process.exit(1);
  }
  // Strip trailing slash
  merged.server = String(merged.server).replace(/\/+$/, '');
  // Persist for next run.
  try {
    fs.writeFileSync(CONFIG_PATH, JSON.stringify({
      server: merged.server, token: merged.token, name: merged.name,
      ffmpeg: merged.ffmpeg, ffprobe: merged.ffprobe, tmpDir: merged.tmpDir,
      noHw: merged.noHw,
    }, null, 2));
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

function buildFfmpegArgs(cfg, srcPath, outDir, caps) {
  const enc = caps.picked || 'libx264';
  const isSw = enc === 'libx264';
  // Preset naming differs between software + GPU encoders.
  const preset = isSw ? 'veryfast'
    : enc === 'h264_nvenc' ? 'p4'      // NVENC: p1 fastest .. p7 slowest
    : enc === 'h264_qsv'   ? 'veryfast'
    : enc === 'h264_amf'   ? 'speed'
    : /* videotoolbox */    null;

  const args = [
    '-y', '-hide_banner', '-loglevel', 'error',
    '-progress', 'pipe:2', '-nostats',
  ];

  // Hardware decode where the encoder is hardware too (optional speed boost).
  if (enc === 'h264_nvenc') args.push('-hwaccel', 'cuda');
  else if (enc === 'h264_qsv') args.push('-hwaccel', 'qsv');

  args.push('-i', srcPath,
    '-map', '0:v:0', '-map', '0:v:0', '-map', '0:v:0',
    '-map', '0:a:0?', '-map', '0:a:0?', '-map', '0:a:0?',
    '-c:v', enc,
  );
  if (preset) args.push('-preset', preset);
  if (isSw) args.push('-profile:v', 'main');
  args.push(
    '-pix_fmt', 'yuv420p',
    '-sc_threshold', '0',
    '-g', '48', '-keyint_min', '48',

    '-s:v:0', '854x480',   '-b:v:0', '800k',  '-maxrate:v:0', '856k',  '-bufsize:v:0', '1200k',
    '-s:v:1', '1280x720',  '-b:v:1', '2500k', '-maxrate:v:1', '2675k', '-bufsize:v:1', '3750k',
    '-s:v:2', '1920x1080', '-b:v:2', '5000k', '-maxrate:v:2', '5350k', '-bufsize:v:2', '7500k',

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
  );
  return args;
}

function runFfmpegWithProgress(cfg, args, durationSec, onProgress) {
  return new Promise((resolve, reject) => {
    const child = spawn(cfg.ffmpeg, args, { stdio: ['ignore', 'ignore', 'pipe'] });
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
