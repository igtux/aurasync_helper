# Agent deployment guide — AuraSync helper worker

You are an AI coding agent (e.g. Claude Code) running on a **Windows** machine that the user wants to use as a remote transcoding node for AuraSync. Your goal: get `aurasync-worker.js` registered with the AuraSync server and running reliably, ideally with hardware-accelerated ffmpeg.

The user has already created (or will create) a worker token in the AuraSync admin panel. Your job is to install dependencies, configure, and start the worker.

> **Tone**: Be terse. Confirm each step succeeded before moving on. Do **not** install anything globally without telling the user. Treat the worker token as a secret — never echo it back into chat or write it anywhere outside `worker.config.json`.

## Inputs you will need

Ask the user for these once, up front:

1. **Server URL** — e.g. `https://aurasync.erpaura.ge`. Must be reachable from this machine.
2. **Worker token** — a string starting with `aw_`. The user generates it in the admin panel under `Workers` → `+ New worker`. It is shown **once**; the server only stores its hash. If they don't have one, point them at `<server>/admin#workers`.
3. **Worker name** (optional) — defaults to the machine's hostname. Useful when running multiple workers.

## Step 1 — verify Node.js

Run:

```powershell
node --version
```

Acceptable: `v18.x.x` or newer.

If missing or older:

```powershell
winget install OpenJS.NodeJS.LTS
```

After install, open a **new** PowerShell window so PATH refreshes, then re-check.

## Step 2 — verify ffmpeg + ffprobe

```powershell
ffmpeg -version
ffprobe -version
```

If either is missing:

```powershell
winget install Gyan.FFmpeg
```

This installs into `%LOCALAPPDATA%\Microsoft\WinGet\Packages\Gyan.FFmpeg_*\ffmpeg-*-essentials_build\bin` and adds it to PATH. Open a new PowerShell window, re-check.

If `winget` isn't available, fall back to direct download from <https://www.gyan.dev/ffmpeg/builds/> (`release-essentials.zip`), extract to `C:\ffmpeg`, add `C:\ffmpeg\bin` to PATH via *System Properties → Environment Variables*.

## Step 3 — detect hardware encoder (informational)

```powershell
ffmpeg -hide_banner -encoders 2>&1 | Select-String "h264_nvenc|h264_qsv|h264_amf"
```

| Output line | Meaning |
|---|---|
| `h264_nvenc` | NVIDIA GPU encoder available (best for transcoding) |
| `h264_qsv` | Intel QuickSync available |
| `h264_amf` | AMD AMF available |
| (nothing) | Software encoding only — workable but slower |

Tell the user which one was detected. The worker auto-picks the fastest at runtime.

If you see no hardware encoder but the user has an NVIDIA GPU, they likely need to update the GPU driver (NVENC ships with the driver, not with ffmpeg).

## Step 4 — fetch the worker

```powershell
cd $env:USERPROFILE
git clone https://github.com/igtux/aurasync_helper.git
cd aurasync_helper
```

If `git` isn't installed: `winget install Git.Git` then re-run.

## Step 5 — first run

Run **once** with full args; the worker writes a `worker.config.json` so you don't need to pass them again.

```powershell
node aurasync-worker.js --server "<SERVER_URL>" --token "<WORKER_TOKEN>" --name "<WORKER_NAME>"
```

Confirm the boot output shows:

- `ffmpeg: ffmpeg version ...`
- `encoder: h264_nvenc (hardware-accelerated)` *(or whatever was detected)*
- `registered as worker xxxxxxxx ("<WORKER_NAME>")`

If you see `registered`, the worker is now in the server's pool and will claim the next queued transcode job. Tell the user.

If `register failed: 401` — token is wrong or revoked; ask the user to mint a fresh one.

If `register failed: ENOTFOUND` / connection refused — server URL is unreachable from this machine.

## Step 6 — install as a Windows service (optional)

If the user wants the worker to run on boot without an open console window, install it as a service.

### Option A — NSSM (recommended)

```powershell
winget install NSSM.NSSM
# Open a NEW elevated PowerShell after install
nssm install AuraSyncWorker "C:\Program Files\nodejs\node.exe" "$env:USERPROFILE\aurasync_helper\aurasync-worker.js"
nssm set AuraSyncWorker AppDirectory "$env:USERPROFILE\aurasync_helper"
nssm set AuraSyncWorker AppStdout "$env:USERPROFILE\aurasync_helper\worker.log"
nssm set AuraSyncWorker AppStderr "$env:USERPROFILE\aurasync_helper\worker.log"
nssm set AuraSyncWorker Start SERVICE_AUTO_START
nssm start AuraSyncWorker
```

Verify with `Get-Service AuraSyncWorker` — `Status: Running`.

The worker will read `worker.config.json` from the AppDirectory (which you set in step 5). No token needs to live in the service config.

### Option B — Task Scheduler

Create a task that triggers *At log on of any user* / *At system startup*, action `node.exe "%USERPROFILE%\aurasync_helper\aurasync-worker.js"`, "Start in" set to the worker folder.

## Step 7 — verify end-to-end

1. Tell the user to open the AuraSync admin panel → Workers tab.
2. Their worker should show as **● online** with the right OS / cores / encoder.
3. If they upload a test movie now, this worker should claim it within ~5 seconds. Watch the log file (`worker.log`) for `claimed job ...` and progress reports.

## Troubleshooting reference

| Symptom | Likely cause | Fix |
|---|---|---|
| `ffmpeg not runnable` | not on PATH | `where ffmpeg` to confirm; install via `winget install Gyan.FFmpeg`; restart shell |
| `register failed: 401` | bad/revoked token | Mint a fresh token in admin panel |
| Worker stays online but never claims | no jobs queued | Normal — only claims when admin uploads a movie |
| `PUT ... → 403 SignatureDoesNotMatch` | clock skew vs server | `w32tm /resync` to fix Windows clock |
| Disk fills up under tmp | failed jobs leave staging dirs | Delete `%TEMP%\aurasync-worker\*`; the worker normally cleans these on success |
| GPU encoder not detected | old driver | Update NVIDIA / Intel / AMD driver; restart |

## Things you must NOT do

- Do **not** commit `worker.config.json` to git — it contains the worker token.
- Do **not** edit `aurasync-worker.js` to "improve" it. The protocol is fixed by the server. If the user reports a bug, file an issue, don't patch locally.
- Do **not** run multiple workers from the same `worker.config.json` directory on the same machine — they'll race on the same token. If the user wants multiple workers per box (rare), use separate folders, each with its own token.
- Do **not** share the worker token. If the user pastes it into chat, scrub it from any future references.

## When you're done

Report to the user:

1. Whether installation succeeded.
2. Which encoder was picked (`h264_nvenc` / `h264_qsv` / etc).
3. Whether the worker is registered and online (you can verify by looking at the worker's terminal output for `registered as worker ...`).
4. Whether you set it up as a service.
5. Any warnings (e.g., software-only encoding because GPU driver is old).
