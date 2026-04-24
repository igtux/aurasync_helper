# aurasync_helper — remote transcoding worker for AuraSync

A single-file Node.js CLI that offloads ffmpeg work from the main AuraSync server. Polls the server for transcode jobs, downloads the source directly from R2 (source never leaves the catalogue's object store), runs ffmpeg locally with hardware acceleration if available, and uploads the HLS ladder back to R2.

**Platforms**: Windows / macOS / Linux. Anywhere Node 18+ and ffmpeg run.

> 🤖 **Deploying with an AI agent (Claude Code, Cursor, etc.)?** Read [`AGENTS.md`](./AGENTS.md) — it's a step-by-step playbook the agent can follow end-to-end on a fresh Windows box.

## Install

1. **Install Node.js 18+** — https://nodejs.org (LTS is fine).
2. **Install ffmpeg** — must be on `PATH` or pass `--ffmpeg <path>`.
   - Windows: https://www.gyan.dev/ffmpeg/builds/ (grab a `release-essentials` zip), extract, add `<ffmpeg>\bin` to your PATH. Or just `winget install Gyan.FFmpeg`.
   - Quick test: `ffmpeg -version` and `ffprobe -version` should both work in your terminal.
3. **Clone this repo**:
   ```
   git clone https://github.com/igtux/aurasync_helper.git
   cd aurasync_helper
   ```
   (Only `aurasync-worker.js` is required; the rest is docs + dev tooling.)

## Get a worker token

In the AuraSync admin panel (`https://<your-aurasync>/admin` → Workers tab):

1. Click **"New worker"**, give it a name (e.g. `gaming-pc`).
2. Copy the token shown **once** (starts with `aw_...`). Server only stores the hash; you can't see it again.

## Run

```bash
# First time — pass config on the command line; it's written to worker.config.json
node aurasync-worker.js --server https://aurasync.erpaura.ge --token aw_... --name gaming-pc

# Subsequent runs — just:
node aurasync-worker.js
```

### Two modes: **poll** vs **local ingest**

The worker supports two ways of getting work:

**Poll mode (default)** — the worker claims any queued transcode job from the server. Sources for poll-mode jobs live in R2 (e.g. when an admin uploaded a movie via the browser). The worker downloads the source from R2, transcodes, uploads HLS back to R2.

**Local ingest** — the source file is already on the worker's disk (you downloaded it locally, or it came from a NAS/share). Skip the R2 round-trip entirely:

```bash
# See what the server is waiting for
node aurasync-worker.js requests

# Transcode + publish in one shot (no upload of the source)
node aurasync-worker.js ingest /path/to/movie.mkv --title <titleId>
node aurasync-worker.js ingest /path/to/s1e2.mkv --title <titleId> --episode <episodeId>
```

Local ingest never copies the source anywhere — ffmpeg reads it in place, and only the HLS output is pushed to R2. Useful when the worker and the source share a disk (same machine, or a mounted NAS).

Environment variables also work: `AURASYNC_SERVER`, `AURASYNC_WORKER_TOKEN`.

On boot you'll see something like:

```
==== AuraSync worker "gaming-pc" ====
   server: https://aurasync.erpaura.ge
   tmpDir: C:\Users\you\AppData\Local\Temp\aurasync-worker
   ffmpeg: ffmpeg version 7.0.2-full_build-www.gyan.dev ...
  encoder: h264_nvenc (hardware-accelerated)
     cpus: 12   mem: 32.0 GB

[hh:mm:ss] registered as worker 4b8c3fe1 ("gaming-pc")
```

The worker then sits in a poll loop. When a transcode job is queued on the server, this machine claims it and runs ffmpeg. You'll see per-file upload progress in the terminal; the AuraSync admin UI shows the live % too.

## Options

```
--server <url>      AuraSync server base URL
--token <token>     Worker token (shown once at admin panel creation)
--name <name>       Human-readable name (default: hostname)
--ffmpeg <path>     Path to ffmpeg binary (default: "ffmpeg" on PATH)
--ffprobe <path>    Path to ffprobe binary (default: "ffprobe" on PATH)
--tmp <dir>         Working dir for downloads + encoding (default: OS tmpdir)
--no-hw             Force software encoding even if a GPU encoder is present
--help
```

## Hardware encoders

On boot the worker runs `ffmpeg -encoders` and picks the first available of:

| Encoder | Hardware |
|---|---|
| `h264_nvenc` | NVIDIA GPU (GeForce GTX 10-series and up) |
| `h264_qsv` | Intel QuickSync (integrated GPU on most modern Intel CPUs) |
| `h264_amf` | AMD AMF (Radeon GPUs) |
| `h264_videotoolbox` | Apple Silicon / macOS |
| `libx264` | Software (always available; fallback) |

A hardware encoder is ~5-10× faster than `libx264 -preset veryfast`. On NVIDIA, a 2-hour 1080p movie encodes in 5-10 minutes instead of 45+.

## Running as a background service (Windows)

### Option A — NSSM (Non-Sucking Service Manager)

```powershell
# Download nssm.exe from nssm.cc, then:
nssm install AuraSyncWorker "C:\Program Files\nodejs\node.exe" "C:\path\to\aurasync-worker.js"
nssm set AuraSyncWorker AppDirectory "C:\path\to\worker"
nssm start AuraSyncWorker
```

The worker reads config from `worker.config.json` in `AppDirectory`, so the first-run CLI config must already exist.

### Option B — Task Scheduler

Create a task that runs `node aurasync-worker.js` at logon, with "Start in" set to the worker folder.

## Security notes

- The worker's token is as sensitive as a password — it grants the ability to claim transcode jobs and produce HLS that ends up in your catalogue. Keep it out of source control.
- The worker never sees your R2 credentials; it only gets short-lived presigned URLs (1h GET for the source, 1h PUT for each output file).
- Revoking a token in the admin panel is immediate — the worker's next API call will fail with 401 and it'll exit.

## Troubleshooting

| Problem | Check |
|---|---|
| "ffmpeg not runnable" | Put ffmpeg on PATH, or use `--ffmpeg "C:\ffmpeg\bin\ffmpeg.exe"` |
| Worker connects, claims nothing | Admin console → Workers tab: is it marked online? If yes, it's fine — no jobs queued |
| "PUT xxx → 403" | R2 bucket CORS might be missing `PUT` from your server's origin; re-check in the Cloudflare dashboard |
| Everything stuck at 0% | Check `tmpDir` has enough free space (transcode output can be 1.5× the source size) |
