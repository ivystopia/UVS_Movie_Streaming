# UVS Movie Streaming

`uvs_movie_stream` plays a movie file at the specified time, with a countdown.

The intended use-case is a Discord movie stream where the movie itself must begin on the exact intended second, but it can do other types of scheduling too.

## What it does

At runtime the script:

1. loads `config.toml`
2. generates or reuses a cached countdown video
3. builds a temporary two-item VLC playlist
4. launches VLC
5. preloads the countdown in a paused-at-zero state through MPRIS
6. starts playback at the requested time, or immediately if no start time was requested

## Countdown format

The current countdown generator uses:

- a user-supplied background image such as `static/background.svg` or `static/background.png`
- a configurable font resolved via `fc-match`
- white fixed-slot digits with a black outline
- separate `mm` and `ss` blocks, with no colon
- one unique frame per visible countdown second
- `countdown_seconds + 1` frames spread across the requested countdown duration at a low rational frame rate
- `libx264rgb -preset ultrafast -crf 0`

This keeps generation fast while still including `00:00` as the terminal frame.

## Requirements

- Debian or another GNU/Linux desktop with a graphical session
- `python3`
- `ffmpeg`
- `ffprobe` if you use `--music`
- `fc-match`
- `vlc`
- `qdbus6`

For local linting, the repo expects the virtualenv:

```bash
~/.virtualenvs/uvs_movie_streaming
```

## Background asset

The repository does not include a background image.

Before using the scheduler, place your own licensed background file in `static/` and point
[config.toml](/home/ivy/Videos/UVS_Movie_Streaming/config.toml) at it. For example:

```bash
static/background.svg
```

The background file itself is intentionally Git-ignored. If the configured file is missing, the
script prints a warning and falls back to a plain black background.

The countdown font is also configured in [config.toml](/home/ivy/Videos/UVS_Movie_Streaming/config.toml)
via a font pattern such as `Comic Shanns Mono` or `DejaVu Sans Mono`.

## Repo layout

- [schedule_uvs_movie_stream.py](/home/ivy/Videos/UVS_Movie_Streaming/schedule_uvs_movie_stream.py): main entry point
- [config.toml](/home/ivy/Videos/UVS_Movie_Streaming/config.toml): runtime defaults
- [static](/home/ivy/Videos/UVS_Movie_Streaming/static): local source assets, including the user-supplied background image
- cache: generated countdown videos, created on demand and ignored by Git
- [lint.sh](/home/ivy/Videos/UVS_Movie_Streaming/lint.sh): local lint/type-check helper

## CLI

Required:

- `-v`, `--video FILE`

Optional scheduling:

- `--countdown-start HH:MM[:SS]`
- `--movie-start HH:MM[:SS]`

Use at most one scheduling option. If you omit both, playback begins immediately.

Optional countdown generation:

- `--countdown-length MM:SS`
- `--music FILE`
- `--force`
- `--resolution WIDTHxHEIGHT`
- `--rebuild-countdown-cache`

Optional VLC overrides:

- `--subtitle-track N`
- `--display NAME`

`--display` is currently accepted for compatibility only. The script no longer tries to force VLC
onto a specific monitor, because that was not reliable under the target Plasma Wayland session.

`--subtitle-track` is 1-based within the movie file's subtitle streams. `1` means the first
subtitle stream in the file. `0` disables subtitle selection.

The built-in help is the source of truth for exact option wording:

```bash
./schedule_uvs_movie_stream.py --help
```

## Examples

Start immediately with the default 5-minute countdown:

```bash
./schedule_uvs_movie_stream.py \
  --video /path/to/movie.mkv
```

Start the countdown at a specific local time:

```bash
./schedule_uvs_movie_stream.py \
  --video /path/to/movie.mkv \
  --countdown-start 19:55
```

Make the movie begin at an exact local time with an explicit 5-minute countdown:

```bash
./schedule_uvs_movie_stream.py \
  --video /path/to/movie.mkv \
  --movie-start 20:00 \
  --countdown-length 05:00
```

Let the script derive the countdown length automatically from the remaining time until movie start.
This only happens when `--music` is also supplied:

```bash
./schedule_uvs_movie_stream.py \
  --video /path/to/movie.mkv \
  --movie-start 20:00 \
  --music /path/to/music.flac
```

Attach music whose end should line up with the end of the countdown:

```bash
./schedule_uvs_movie_stream.py \
  --video /path/to/movie.mkv \
  --movie-start 20:00 \
  --music /path/to/music.flac
```

Force regeneration of a cached countdown:

```bash
./schedule_uvs_movie_stream.py \
  --video /path/to/movie.mkv \
  --movie-start 20:00 \
  --rebuild-countdown-cache
```

## Scheduling rules

- All wall-clock times are interpreted in local time.
- If a requested `--countdown-start` or `--movie-start` has already passed today, the script assumes you mean tomorrow.
- `--countdown-start` means the countdown begins at that exact second.
- `--movie-start` means the movie begins at that exact second.
- If `--movie-start` is used without `--countdown-length`, the script keeps the normal default countdown length unless `--music` is also supplied.
- If both `--movie-start` and `--music` are used without `--countdown-length`, the script derives the countdown length from the remaining time minus a conservative render allowance.
- If the requested movie start does not leave enough time for generation plus the full countdown, the script fails early and reports the earliest workable movie-start time.
- For scheduled runs, the startup summary reports both the minimum required pre-start window and the actual available window before the countdown begins.

## Music rules

- If `--music` is omitted, the countdown video is video-only.
- If `--music` is supplied and `--countdown-length` is omitted in immediate mode, the countdown length defaults to the music duration plus a 1-second buffer, rounded up.
- The end of the music always aligns to the end of the countdown.
- By default, the script rejects countdowns that would cut off the beginning of the music.
- `--force` allows truncating the start of the music.
- Generated countdowns with music store audio as PCM for stable playback onset in VLC.

## Countdown cache

- Generated countdowns are stored in `cache/`.
- The default `05:00` countdown at the default resolution uses `countdown_filename` from [config.toml](/home/ivy/Videos/UVS_Movie_Streaming/config.toml).
- Other durations, resolutions, or music inputs are cached as separate derived files.
- Cached files are reused unless `--rebuild-countdown-cache` is supplied.

## Performance

The current static low-frame-rate generator scales approximately linearly with countdown length.

Measured on this machine at `1920x1080`, no music:

- `00:30`: `1.007s`
- `01:00`: `1.271s`
- `02:00`: `1.824s`
- `05:00`: `3.447s`
- `10:00`: `5.071s`
- `20:00`: `9.560s`
- `40:00`: `21.268s`

Approximate fit:

- `render_time ≈ 0.57s + 0.00837s * countdown_seconds`

That is why the movie-start calculation uses a relatively small render buffer on the current static pipeline.

## Validation

Run local checks with:

```bash
./lint.sh
```

That helper runs Python compilation, `flake8`, `mypy`, and `pyright` inside the configured
virtualenv.
