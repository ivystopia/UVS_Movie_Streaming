# UVS Movie Streaming Repo

This file is for agents working in this repository. It documents the current operational truth of the codebase. For user-facing usage guidance, see [README.md](/home/ivy/Videos/UVS_Movie_Streaming/README.md).

Do not read `CONTEXT.md` by default. It is intentionally high-context history and is now Git-ignored. Use it only when the task clearly needs prior rationale, benchmark history, or abandoned approaches.

## Purpose

The repo contains a Python tool that starts VLC with:

1. a generated countdown video
2. a user-supplied movie file

Playback can begin immediately, at an exact countdown start time, or at an exact movie start time.

## Entry points

- [schedule_uvs_movie_stream.py](/home/ivy/Videos/UVS_Movie_Streaming/schedule_uvs_movie_stream.py): main script
- [config.toml](/home/ivy/Videos/UVS_Movie_Streaming/config.toml): runtime defaults
- [lint.sh](/home/ivy/Videos/UVS_Movie_Streaming/lint.sh): local compile/lint/type-check helper
- [pyproject.toml](/home/ivy/Videos/UVS_Movie_Streaming/pyproject.toml): packaging and dev dependencies

## Current runtime model

The scheduler currently:

1. loads `config.toml`
2. validates the movie path and countdown request
3. generates or reuses a cached countdown video under `cache/`
4. builds a temporary two-item XSPF playlist
5. launches VLC immediately
6. waits for `org.mpris.MediaPlayer2.vlc` over `qdbus6`
7. pauses and rewinds the countdown to position `0`
8. waits until the target start time, if one was requested
9. sends `Play` over MPRIS

If neither `--countdown-start` nor `--movie-start` is supplied, the script launches VLC and leaves playback to begin immediately.

## CLI contract

Required:

- `-v`, `--video FILE`

Optional mutually exclusive scheduling:

- `--countdown-start HH:MM[:SS]`
- `--movie-start HH:MM[:SS]`

Optional countdown-generation arguments:

- `--countdown-length MM:SS`
- `--music FILE`
- `--force`
- `--resolution WIDTHxHEIGHT`
- `--rebuild-countdown-cache`

Optional VLC overrides:

- `--subtitle-track N`
- `--display NAME`

Compatibility aliases still accepted:

- `--screen` for `--display`
- `--regenerate-countdown-video` for `--rebuild-countdown-cache`

## Scheduling semantics

- All wall-clock times are local time.
- If a requested wall-clock time has already passed today, the script rolls it to tomorrow.
- `--countdown-start` means the countdown begins at that exact second.
- `--movie-start` means the movie begins at that exact second.
- If `--movie-start` is supplied without `--countdown-length`, the script derives the countdown length from the time remaining until movie start minus an estimated render allowance.
- If `--movie-start` does not leave enough future time for generation plus countdown playback, the script fails early and reports the earliest workable movie-start time.

## Countdown generation

Source assets live in [static](/home/ivy/Videos/UVS_Movie_Streaming/static). Generated countdown videos belong in `cache/`, which is intentionally Git-ignored and may not exist until the first generation run.

Current source asset expectations:

- a user-supplied local background image under `static/`, configured via `files.countdown_background`

The repository does not ship a background asset. `static/background.svg` is the default local path and is intentionally Git-ignored, but the filename is configurable in [config.toml](/home/ivy/Videos/UVS_Movie_Streaming/config.toml). If the configured file is missing, countdown generation falls back to a plain black background and emits a warning instead of failing.

Current generator behavior:

- static background
- no animated elements
- configurable font resolved via `fc-match`
- white fixed-slot digits with black outline
- separate `mm` and `ss` blocks, no colon
- `countdown_seconds + 1` frames at a literal `1 fps`
- `libx264rgb -preset ultrafast -crf 0`

Current countdown constraints:

- allowed range: `00:30` to `59:59`
- default length when not otherwise specified: `05:00`
- default resolution comes from `config.toml`
- if `--music` is supplied and `--countdown-length` is omitted in immediate mode, the countdown defaults to the music duration plus one second, rounded up

Music behavior:

- countdown audio is optional
- when present, the end of the music is aligned to the end of the countdown
- by default, the script rejects cases that would trim the start of the music
- `--force` allows truncating the start of the music
- generated countdowns with music store PCM audio for stable playback onset in VLC

## Cache behavior

- the default `05:00` countdown at the configured default resolution uses `countdown_filename` from `config.toml`
- other countdown variants derive their own filenames under `cache/`
- cached outputs are reused unless `--rebuild-countdown-cache` is supplied

## Config contract

[config.toml](/home/ivy/Videos/UVS_Movie_Streaming/config.toml) currently defines:

- `playback.fullscreen_display`
- `playback.subtitle_track`
- `files.static_dirname`
- `files.cache_dirname`
- `files.countdown_filename`
- `files.countdown_background`
- `files.countdown_resolution`
- `files.countdown_font`
- `logging.wrapper_log`
- `logging.vlc_log`
- `tools.vlc_binary`
- `tools.qdbus_binary`

## Tooling

- [lint.sh](/home/ivy/Videos/UVS_Movie_Streaming/lint.sh) runs `py_compile`, `flake8`, `mypy`, and `pyright`
- the expected virtualenv is `~/.virtualenvs/uvs_movie_streaming`
- [.flake8](/home/ivy/Videos/UVS_Movie_Streaming/.flake8) ignores `E501`

## Maintenance rules

When repo behavior changes:

- update this file so it reflects the current codebase
- update [README.md](/home/ivy/Videos/UVS_Movie_Streaming/README.md) if user-facing behavior or examples changed
- update `CONTEXT.md` only for high-context historical notes, benchmarks, and rationale
- run [lint.sh](/home/ivy/Videos/UVS_Movie_Streaming/lint.sh) as the final step before considering the change finished
