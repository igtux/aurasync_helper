@echo off
REM AuraSync worker — Windows quick launch.
REM Double-click this after you've created worker.config.json (first run via CLI).
REM Or edit below to hard-code server + token.

setlocal

REM --- optional: uncomment + fill to hard-code rather than use worker.config.json ---
REM set AURASYNC_SERVER=https://aurasync.erpaura.ge
REM set AURASYNC_WORKER_TOKEN=aw_paste_your_token_here

cd /d "%~dp0"
node aurasync-worker.js
pause
