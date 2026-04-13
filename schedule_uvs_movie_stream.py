#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import tomllib
import xml.etree.ElementTree as ET
from dataclasses import dataclass, replace
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont
from PyQt6.QtCore import qInstallMessageHandler
from PyQt6.QtGui import QGuiApplication


APP_NAME = "uvs_movie_stream"
VERSION = "1.0.0"
MPRIS_SERVICE = "org.mpris.MediaPlayer2.vlc"
MPRIS_PATH = "/org/mpris/MediaPlayer2"
TIME_PATTERN = re.compile(
    r"^(?P<hour>[01]\d|2[0-3]):(?P<minute>[0-5]\d)(?::(?P<second>[0-5]\d))?$"
)
COUNTDOWN_PATTERN = re.compile(r"^(?P<minute>[0-5]\d):(?P<second>[0-5]\d)$")
RESOLUTION_PATTERN = re.compile(r"^(?P<width>[1-9]\d*)x(?P<height>[1-9]\d*)$")
XSPF_NS = "http://xspf.org/ns/0/"
VLC_NS = "http://www.videolan.org/vlc/playlist/ns/0/"
ET.register_namespace("", XSPF_NS)
ET.register_namespace("vlc", VLC_NS)

COUNTDOWN_DEFAULT_SECONDS = 5 * 60
COUNTDOWN_MIN_SECONDS = 30
COUNTDOWN_MAX_SECONDS = 59 * 60 + 59
COUNTDOWN_RENDER_ALLOWANCE_SECONDS = 5
COUNTDOWN_RENDER_ESTIMATE_BASE_SECONDS = 1.5
COUNTDOWN_RENDER_ESTIMATE_PER_SECOND = 0.0085
COUNTDOWN_MUSIC_MUX_ALLOWANCE_SECONDS = 2
COUNTDOWN_RENDER_SAFETY_MARGIN_SECONDS = 2
COUNTDOWN_TARGET_FPS = 1
DEFAULT_RESOLUTION = "1920x1080"
COUNTDOWN_VIDEO_CODEC = "libx264rgb"
COUNTDOWN_VIDEO_PRESET = "ultrafast"
COUNTDOWN_VIDEO_CRF = "0"
REFERENCE_WIDTH = 1920
REFERENCE_HEIGHT = 1080
REFERENCE_CENTER_X = 960
REFERENCE_CENTER_Y = 540
REFERENCE_OUTER_CIRCLE_RADIUS = 295
REFERENCE_SIDE_MARGIN_X = 130
REFERENCE_SIDE_MARGIN_Y = 140
REFERENCE_CIRCLE_GAP_X = 40
TEXT_FILL = "white"
TEXT_STROKE_FILL = "black"
REFERENCE_TEXT_STROKE_WIDTH = 10
FFMPEG_BINARY = "ffmpeg"
FFPROBE_BINARY = "ffprobe"
FC_MATCH_BINARY = "fc-match"
FONT_FAMILY = "Comic Shanns Mono"


class SchedulerError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Config:
    fullscreen_display: str
    subtitle_track: int
    static_dirname: str
    cache_dirname: str
    countdown_filename: str
    countdown_background: str
    countdown_resolution: "VideoSize"
    wrapper_log: Path
    vlc_log: Path
    vlc_binary: Path
    qdbus_binary: Path

    @classmethod
    def load(cls, path: Path) -> "Config":
        if not path.is_file():
            raise SchedulerError(f"Config file not found: {path}")

        try:
            with path.open("rb") as handle:
                raw = tomllib.load(handle)
        except tomllib.TOMLDecodeError as exc:
            raise SchedulerError(f"Invalid TOML in {path}: {exc}") from exc

        playback = require_table(raw, "playback")
        files = require_table(raw, "files")
        logging_cfg = require_table(raw, "logging")
        tools = require_table(raw, "tools")

        return cls(
            fullscreen_display=require_str(playback, "fullscreen_display"),
            subtitle_track=require_int(playback, "subtitle_track"),
            static_dirname=require_str(files, "static_dirname"),
            cache_dirname=require_str(files, "cache_dirname"),
            countdown_filename=require_str(files, "countdown_filename"),
            countdown_background=require_str(files, "countdown_background"),
            countdown_resolution=parse_resolution(
                require_str(files, "countdown_resolution"),
                source="config key 'countdown_resolution'",
                error_type=SchedulerError,
            ),
            wrapper_log=Path(require_str(logging_cfg, "wrapper_log")).expanduser(),
            vlc_log=Path(require_str(logging_cfg, "vlc_log")).expanduser(),
            vlc_binary=Path(require_str(tools, "vlc_binary")).expanduser(),
            qdbus_binary=Path(require_str(tools, "qdbus_binary")).expanduser(),
        )

    def with_overrides(
        self,
        *,
        fullscreen_display: str | None,
        subtitle_track: int | None,
    ) -> "Config":
        updated = self
        if fullscreen_display is not None:
            updated = replace(updated, fullscreen_display=fullscreen_display)
        if subtitle_track is not None:
            updated = replace(updated, subtitle_track=subtitle_track)
        return updated


@dataclass(frozen=True, slots=True)
class Inputs:
    movie_path: Path
    start_at: datetime | None
    countdown_seconds: int
    countdown_resolution: "VideoSize"
    music_path: Path | None
    regenerate_countdown_video: bool
    force_music_truncation: bool


@dataclass(frozen=True, slots=True)
class VideoSize:
    width: int
    height: int

    @property
    def text(self) -> str:
        return f"{self.width}x{self.height}"


@dataclass(frozen=True, slots=True)
class Display:
    index: int
    name: str
    x: int
    y: int
    width: int
    height: int


@dataclass(frozen=True, slots=True)
class CountdownRenderPlan:
    slot_width: int
    slot_height: int
    left_x0: int
    right_x0: int
    y0: int
    cell_cache: dict[str, Image.Image]


@dataclass(frozen=True, slots=True)
class AudioStreamInfo:
    codec_name: str
    sample_rate: int
    channels: int
    channel_layout: str | None


@dataclass(frozen=True, slots=True)
class CountdownAudioPlan:
    music_duration: float
    trim_start: float
    start_delay: float
    encoder: str


def xspf_tag(name: str) -> str:
    return f"{{{XSPF_NS}}}{name}"


def vlc_tag(name: str) -> str:
    return f"{{{VLC_NS}}}{name}"


def normalize_bbox(bbox: tuple[float, float, float, float]) -> tuple[int, int, int, int]:
    left, top, right, bottom = bbox
    return (round(left), round(top), round(right), round(bottom))


class MprisClient:
    # Keep MPRIS access in one place so scheduler flow stays readable.
    def __init__(self, qdbus_binary: Path, logger: logging.Logger) -> None:
        self.qdbus_binary = qdbus_binary
        self.logger = logger

    def available(self) -> bool:
        result = subprocess.run(
            [str(self.qdbus_binary)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
        )
        return MPRIS_SERVICE in {line.strip() for line in result.stdout.splitlines()}

    def wait_until(self, deadline_epoch: float) -> bool:
        while time.time() < deadline_epoch:
            if self.available():
                return True
            time.sleep(0.25)
        return False

    def play(self) -> None:
        result = self._call(
            "org.mpris.MediaPlayer2.Player.Play",
            capture_output=False,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            self.logger.error("Failed to trigger playback: %s", stderr or "<no stderr>")
            raise SchedulerError(
                "Failed to trigger playback. VLC was left running for inspection. "
                "See the wrapper and VLC logs for details."
            )

    def pause(self) -> None:
        result = self._call(
            "org.mpris.MediaPlayer2.Player.Pause",
            capture_output=False,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            self.logger.error(
                "Failed to pause VLC during preload: %s",
                stderr or "<no stderr>",
            )
            raise SchedulerError(
                "Failed to pause VLC during preload. "
                "See the wrapper and VLC logs for details."
            )

    def seek_relative(self, offset: int) -> None:
        result = self._call(
            "org.mpris.MediaPlayer2.Player.Seek",
            str(offset),
            capture_output=False,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            self.logger.error(
                "Failed to rewind VLC during preload: %s",
                stderr or "<no stderr>",
            )
            raise SchedulerError(
                "Failed to rewind VLC during preload. "
                "See the wrapper and VLC logs for details."
            )

    def playback_status(self) -> str:
        return self._get_property(
            "org.mpris.MediaPlayer2.Player",
            "PlaybackStatus",
        ).strip()

    def position(self) -> int:
        raw = self._get_property("org.mpris.MediaPlayer2.Player", "Position")
        digits = re.sub(r"[^0-9-]", "", raw)
        if not digits:
            raise SchedulerError("Could not parse VLC playback position from qdbus6 output.")
        return int(digits)

    def _get_property(self, interface: str, prop: str) -> str:
        result = self._call(
            "org.freedesktop.DBus.Properties.Get",
            interface,
            prop,
            capture_output=True,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            self.logger.error(
                "Failed to read MPRIS property %s.%s: %s",
                interface,
                prop,
                stderr or "<no stderr>",
            )
            raise SchedulerError(
                f"Failed to read VLC MPRIS property {interface}.{prop}. "
                "See the wrapper and VLC logs for details."
            )
        return result.stdout

    def _call(
        self,
        method: str,
        *args: str,
        capture_output: bool,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(self.qdbus_binary), MPRIS_SERVICE, MPRIS_PATH, method, *args],
            stdout=subprocess.PIPE if capture_output else subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
        )


class MovieStreamScheduler:
    def __init__(self, config: Config, inputs: Inputs, script_dir: Path) -> None:
        self.config = config
        self.inputs = inputs
        self.script_dir = script_dir
        self.static_dir = self.script_dir / self.config.static_dirname
        self.cache_dir = self.script_dir / self.config.cache_dirname
        self.countdown_path = self.resolve_countdown_path()
        self.background_path = self.static_dir / self.config.countdown_background
        self.run_dir = Path(tempfile.mkdtemp(prefix=f"{APP_NAME}_"))
        self.playlist_path = self.run_dir / self._build_playlist_filename()
        self.logger = build_logger(self.config.wrapper_log)
        self.mpris = MprisClient(self.config.qdbus_binary, self.logger)
        self.display = resolve_display(self.config.fullscreen_display)
        self.vlc_process: subprocess.Popen[bytes] | None = None
        # These gates decide whether cleanup() should undo the preload work.
        self.cleanup_preloaded_vlc_on_exit = False
        self.cleanup_run_dir_on_exit = True
        self._install_signal_handlers()

    def run(self) -> int:
        self.logger.info("Wrapper starting (version %s)", VERSION)
        self.validate()
        self.ensure_countdown_video()
        self.ensure_start_window_remaining()
        self.write_playlist()
        self.ensure_vlc_not_running()
        self.announce_startup()
        self.launch_vlc()

        if self.inputs.start_at is None:
            self.cleanup_preloaded_vlc_on_exit = False
            self.cleanup_run_dir_on_exit = False
            self.logger.info("Immediate mode requested; leaving VLC to play the playlist normally")
            return 0

        if not self.mpris.wait_until(self.inputs.start_at.timestamp()):
            raise SchedulerError(
                f"VLC did not expose {MPRIS_SERVICE} before the scheduled start time. "
                f"See {self.config.vlc_log} for VLC startup output."
            )

        self.logger.info("Detected %s", MPRIS_SERVICE)
        # Put VLC into a deterministic paused-at-zero preload state before waiting.
        self.prepare_preloaded_countdown()
        wait_until(self.inputs.start_at.timestamp())
        self.logger.info("Reached playback trigger time")

        self.cleanup_preloaded_vlc_on_exit = False
        self.cleanup_run_dir_on_exit = False
        self.mpris.play()
        self.logger.info("Playback trigger sent successfully")
        print(f"Playback triggered at {datetime.now():%Y-%m-%d %H:%M:%S.%f}"[:-3])
        return 0

    def cleanup(self) -> None:
        if self.vlc_process is not None:
            if self.cleanup_preloaded_vlc_on_exit:
                if self.vlc_process.poll() is None:
                    self.logger.info("Cleaning up preloaded VLC after wrapper exit")
                    self.vlc_process.terminate()
                    try:
                        self.vlc_process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        self.vlc_process.kill()
                        self.vlc_process.wait(timeout=5)
            else:
                self.logger.info("Leaving VLC running after wrapper exit")

        if self.cleanup_run_dir_on_exit and self.run_dir.exists():
            shutil.rmtree(self.run_dir, ignore_errors=True)

    def validate(self) -> None:
        if not self.inputs.movie_path.is_file():
            raise SchedulerError(f"Movie file does not exist: {self.inputs.movie_path}")
        if self.config.subtitle_track < 0:
            raise SchedulerError(
                f"Subtitle track must be zero or greater: {self.config.subtitle_track}"
            )
        require_executable(self.config.vlc_binary, label="VLC binary")
        require_executable(self.config.qdbus_binary, label="qdbus6 binary")
        require_command_available(FFMPEG_BINARY, label="ffmpeg")
        require_command_available(FC_MATCH_BINARY, label="fc-match")
        if self.inputs.music_path is not None:
            require_command_available(FFPROBE_BINARY, label="ffprobe")

        if self.inputs.start_at is not None and self.inputs.start_at.timestamp() <= time.time():
            raise SchedulerError(
                "Scheduled start time is no longer in the future. "
                "Choose a later time and try again."
            )

    def ensure_start_window_remaining(self) -> None:
        if self.inputs.start_at is None:
            return
        if self.inputs.start_at.timestamp() <= time.time():
            raise SchedulerError(
                "Countdown generation used up the time reserved for rendering before playback "
                "could begin. Choose a later start, reuse an existing cached countdown, or "
                "specify --countdown-length explicitly."
            )

    def write_playlist(self) -> None:
        playlist = ET.Element(xspf_tag("playlist"), version="1")
        ET.SubElement(playlist, xspf_tag("title")).text = "UVS Movie Stream"

        track_list = ET.SubElement(playlist, xspf_tag("trackList"))
        self.append_track(
            track_list,
            self.countdown_path,
            format_countdown_label(self.inputs.countdown_seconds),
            track_id=0,
        )
        self.append_track(
            track_list,
            self.inputs.movie_path,
            self.inputs.movie_path.stem,
            track_id=1,
            subtitle_track=self.config.subtitle_track,
        )

        extension = ET.SubElement(
            playlist,
            xspf_tag("extension"),
            application="http://www.videolan.org/vlc/playlist/0",
        )
        ET.SubElement(extension, vlc_tag("item"), tid="0")
        ET.SubElement(extension, vlc_tag("item"), tid="1")

        ET.ElementTree(playlist).write(
            self.playlist_path,
            encoding="utf-8",
            xml_declaration=True,
        )
        self.logger.info("Generated playlist at %s", self.playlist_path)

    def append_track(
        self,
        track_list: ET.Element,
        media_path: Path,
        title: str,
        *,
        track_id: int,
        subtitle_track: int | None = None,
    ) -> None:
        track = ET.SubElement(track_list, xspf_tag("track"))
        ET.SubElement(track, xspf_tag("location")).text = media_path.resolve().as_uri()
        ET.SubElement(track, xspf_tag("title")).text = title

        extension = ET.SubElement(
            track,
            xspf_tag("extension"),
            application="http://www.videolan.org/vlc/playlist/0",
        )
        ET.SubElement(extension, vlc_tag("id")).text = str(track_id)

        if subtitle_track is not None:
            ET.SubElement(extension, vlc_tag("option")).text = f"sub-track={subtitle_track}"

    def ensure_vlc_not_running(self) -> None:
        if self.mpris.available():
            raise SchedulerError(
                f"VLC already appears to be running on MPRIS ({MPRIS_SERVICE}). "
                "Close VLC before scheduling a new playback."
            )

    def announce_startup(self) -> None:
        movie_start_at = (
            self.inputs.start_at + timedelta(seconds=self.inputs.countdown_seconds)
            if self.inputs.start_at is not None
            else None
        )
        print(f"Generated playlist: {self.playlist_path}")
        print(f"Movie file: {self.inputs.movie_path}")
        print(f"Countdown video: {self.countdown_path}")
        print(f"Countdown length: {format_countdown_mmss(self.inputs.countdown_seconds)}")
        print(f"Countdown resolution: {self.inputs.countdown_resolution.text}")
        print(f"Countdown music: {self.inputs.music_path or 'none'}")
        if self.inputs.music_path is not None:
            audio_plan = build_countdown_audio_plan(
                self.inputs.music_path,
                self.inputs.countdown_seconds,
                allow_music_truncation=self.inputs.force_music_truncation,
            )
            print(f"Music duration: {format_duration_precise(audio_plan.music_duration)}")
            print(f"Silent lead-in: {format_duration_precise(audio_plan.start_delay)}")
            print(f"Music trimmed at start: {format_duration_precise(audio_plan.trim_start)}")
        print(f"Subtitle track: {self.config.subtitle_track}")
        if self.inputs.start_at is None:
            print("Playlist start: immediate")
            print(
                "Movie start: immediate mode "
                f"(the movie begins {format_countdown_mmss(self.inputs.countdown_seconds)} "
                "after playback starts)"
            )
        else:
            print("VLC preload at immediate launch")
            print(f"Playlist start at {self.inputs.start_at:%Y-%m-%d %H:%M:%S}")
            assert movie_start_at is not None
            print(f"Movie start at {movie_start_at:%Y-%m-%d %H:%M:%S}")
        print(
            f"Fullscreen display: {self.display.name} "
            f"(index {self.display.index}, geometry "
            f"{self.display.x},{self.display.y} {self.display.width}x{self.display.height})"
        )
        self.logger.info("Launching VLC immediately")
        if self.inputs.start_at is None:
            self.logger.info("Immediate playback requested")
        else:
            self.logger.info(
                "Scheduled playback trigger for %s",
                self.inputs.start_at.strftime("%Y-%m-%d %H:%M:%S"),
            )
            assert movie_start_at is not None
            self.logger.info(
                "Movie playback will begin at %s after a %s countdown",
                movie_start_at.strftime("%Y-%m-%d %H:%M:%S"),
                format_countdown_mmss(self.inputs.countdown_seconds),
            )
        if self.inputs.music_path is not None:
            audio_plan = build_countdown_audio_plan(
                self.inputs.music_path,
                self.inputs.countdown_seconds,
                allow_music_truncation=self.inputs.force_music_truncation,
            )
            self.logger.info(
                "Countdown audio plan: music_duration=%s silent_lead_in=%s trim_at_start=%s",
                format_duration_precise(audio_plan.music_duration),
                format_duration_precise(audio_plan.start_delay),
                format_duration_precise(audio_plan.trim_start),
            )

    def launch_vlc(self) -> None:
        self.config.vlc_log.parent.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, QT_QPA_PLATFORM="xcb")
        command = [
            str(self.config.vlc_binary),
            "--dbus",
            "--vout",
            "xcb_x11",
            f"--video-x={self.display.x}",
            f"--video-y={self.display.y}",
            "--fullscreen",
            f"--qt-fullscreen-screennumber={self.display.index}",
            str(self.playlist_path),
        ]

        with self.config.vlc_log.open("w", encoding="utf-8") as log_handle:
            self.vlc_process = subprocess.Popen(
                command,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                env=env,
                start_new_session=True,
            )

        self.cleanup_preloaded_vlc_on_exit = True
        self.logger.info("Launched VLC with pid %s", self.vlc_process.pid)

    def prepare_preloaded_countdown(self) -> None:
        # Some VLC starts arrive already playing, so force pause first.
        self.mpris.pause()
        time.sleep(0.05)

        position = self.mpris.position()
        if position > 0:
            # MPRIS seek is relative, so rewind by the current position.
            self.mpris.seek_relative(-position)
            time.sleep(0.05)

        # Reassert pause after the rewind so the trigger point is stable.
        self.mpris.pause()
        status = self.mpris.playback_status() or "<unknown>"
        position = self.mpris.position()
        self.logger.info("Prepared preload state: status=%s position=%s", status, position)

    def _install_signal_handlers(self) -> None:
        def handler(signum: int, _frame: object) -> None:
            raise SystemExit(128 + signum)

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def _build_playlist_filename(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{APP_NAME}_{timestamp}_{os.getpid()}.xspf"

    def resolve_countdown_path(self) -> Path:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        return resolve_countdown_cache_path(
            self.config,
            self.script_dir,
            self.inputs.countdown_seconds,
            self.inputs.countdown_resolution,
            self.inputs.music_path,
        )

    def ensure_countdown_video(self) -> None:
        if self.countdown_path.is_file() and not self.inputs.regenerate_countdown_video:
            return

        movie_start_at = (
            self.inputs.start_at + timedelta(seconds=self.inputs.countdown_seconds)
            if self.inputs.start_at is not None
            else None
        )
        self.logger.info("Generating countdown video at %s", self.countdown_path)
        self.logger.info(
            "Countdown plan: length=%s resolution=%s",
            format_countdown_mmss(self.inputs.countdown_seconds),
            self.inputs.countdown_resolution.text,
        )
        if self.inputs.start_at is not None:
            self.logger.info(
                "Countdown will start at %s",
                self.inputs.start_at.strftime("%Y-%m-%d %H:%M:%S"),
            )
            assert movie_start_at is not None
            self.logger.info(
                "Movie is scheduled to begin at %s",
                movie_start_at.strftime("%Y-%m-%d %H:%M:%S"),
            )
        if self.inputs.music_path is not None:
            audio_plan = build_countdown_audio_plan(
                self.inputs.music_path,
                self.inputs.countdown_seconds,
                allow_music_truncation=self.inputs.force_music_truncation,
            )
            self.logger.info(
                "Countdown audio plan before generation: music_duration=%s "
                "silent_lead_in=%s trim_at_start=%s",
                format_duration_precise(audio_plan.music_duration),
                format_duration_precise(audio_plan.start_delay),
                format_duration_precise(audio_plan.trim_start),
            )
        build_countdown_video(
            self.countdown_path,
            self.inputs.countdown_seconds,
            self.inputs.countdown_resolution,
            self.background_path,
            self.inputs.music_path,
            allow_music_truncation=self.inputs.force_music_truncation,
            logger=self.logger,
        )
        self.logger.info("Generated countdown video at %s", self.countdown_path)


def require_table(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise SchedulerError(f"Config section [{key}] is required.")
    return value


def require_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise SchedulerError(f"Config key {key!r} must be a non-empty string.")
    return value


def require_int(data: dict[str, Any], key: str) -> int:
    value = data.get(key)
    if not isinstance(value, int):
        raise SchedulerError(f"Config key {key!r} must be an integer.")
    return value


def require_executable(path: Path, *, label: str) -> None:
    if not path.is_file():
        raise SchedulerError(f"{label} not found: {path}")
    if not os.access(path, os.X_OK):
        raise SchedulerError(f"{label} is not executable: {path}")


def require_command_available(command: str, *, label: str) -> None:
    if shutil.which(command) is None:
        raise SchedulerError(f"{label} was not found on PATH: {command}")


def parse_movie_path(value: str) -> Path:
    movie_path = Path(value).expanduser().resolve()
    if not movie_path.is_file():
        raise argparse.ArgumentTypeError(f"Movie file does not exist: {movie_path}")
    return movie_path


def parse_music_path(value: str) -> Path:
    music_path = Path(value).expanduser().resolve()
    if not music_path.is_file():
        raise argparse.ArgumentTypeError(f"Music file does not exist: {music_path}")
    return music_path


def parse_start_time(value: str) -> datetime:
    return resolve_next_occurrence(parse_clock_time(value))


def parse_clock_time(value: str) -> dt_time:
    match = TIME_PATTERN.fullmatch(value)
    if not match:
        raise argparse.ArgumentTypeError("Time must be in HH:MM or HH:MM:SS format.")

    second = match.group("second") or "00"
    return dt_time(
        hour=int(match.group("hour")),
        minute=int(match.group("minute")),
        second=int(second),
    )


def parse_countdown_time(value: str) -> int:
    match = COUNTDOWN_PATTERN.fullmatch(value)
    if not match:
        raise argparse.ArgumentTypeError("Countdown must be in MM:SS format.")

    total_seconds = int(match.group("minute")) * 60 + int(match.group("second"))
    if total_seconds < COUNTDOWN_MIN_SECONDS:
        raise argparse.ArgumentTypeError(
            f"Countdown must be at least {format_countdown_mmss(COUNTDOWN_MIN_SECONDS)}."
        )
    if total_seconds > COUNTDOWN_MAX_SECONDS:
        raise argparse.ArgumentTypeError(
            f"Countdown must be at most {format_countdown_mmss(COUNTDOWN_MAX_SECONDS)}."
        )
    return total_seconds


def resolve_next_occurrence(clock_time: dt_time, *, now: datetime | None = None) -> datetime:
    current_time = now or datetime.now()
    occurrence = datetime.combine(current_time.date(), clock_time)
    if occurrence <= current_time:
        occurrence += timedelta(days=1)
    return occurrence


def ceil_to_representable_second(moment: datetime) -> datetime:
    rounded = moment.replace(microsecond=0)
    if rounded < moment:
        rounded += timedelta(seconds=1)
    return rounded


def next_strict_second(moment: datetime) -> datetime:
    rounded = ceil_to_representable_second(moment)
    if rounded <= moment:
        rounded += timedelta(seconds=1)
    return rounded


def estimate_render_allowance_seconds(
    countdown_seconds: int,
    *,
    has_music: bool,
) -> int:
    # Countdown rendering scales roughly with duration, so reserve a conservative
    # window that grows with countdown length instead of assuming a fixed cost.
    estimated = COUNTDOWN_RENDER_ESTIMATE_BASE_SECONDS + (
        COUNTDOWN_RENDER_ESTIMATE_PER_SECOND * countdown_seconds
    )
    if has_music:
        estimated += COUNTDOWN_MUSIC_MUX_ALLOWANCE_SECONDS
    estimated += COUNTDOWN_RENDER_SAFETY_MARGIN_SECONDS
    return max(COUNTDOWN_RENDER_ALLOWANCE_SECONDS, math.ceil(estimated))


def resolve_countdown_cache_path(
    config: Config,
    script_dir: Path,
    countdown_seconds: int,
    countdown_resolution: "VideoSize",
    music_path: Path | None,
) -> Path:
    cache_dir = script_dir / config.cache_dirname
    if (
        countdown_seconds == COUNTDOWN_DEFAULT_SECONDS
        and countdown_resolution == config.countdown_resolution
        and music_path is None
    ):
        return cache_dir / config.countdown_filename

    extension = Path(config.countdown_filename).suffix or ".mkv"
    suffix = ""
    if music_path is not None:
        suffix = f"_music_{build_music_cache_key(music_path)}"
    return cache_dir / (
        f"countdown_{format_countdown_filename_token(countdown_seconds)}"
        f"_{countdown_resolution.text}_{COUNTDOWN_TARGET_FPS}fps{suffix}{extension}"
    )


def derive_movie_timed_countdown(
    clock_time: dt_time,
    *,
    minimum_countdown_seconds: int = COUNTDOWN_MIN_SECONDS,
    has_music: bool = False,
    allow_music_truncation: bool = False,
    now: datetime | None = None,
) -> tuple[int, datetime]:
    current_time = now or datetime.now()
    movie_start = resolve_next_occurrence(clock_time, now=current_time)
    required_countdown_seconds = (
        COUNTDOWN_MIN_SECONDS if allow_music_truncation else minimum_countdown_seconds
    )
    render_allowance_seconds = estimate_render_allowance_seconds(
        required_countdown_seconds,
        has_music=has_music,
    )
    # Auto-derived countdown length and render allowance depend on each other.
    # Iterate until the conservative allowance stabilizes for the derived length.
    for _ in range(8):
        countdown_start = ceil_to_representable_second(
            current_time + timedelta(seconds=render_allowance_seconds)
        )
        countdown_seconds = int((movie_start - countdown_start).total_seconds())
        next_allowance_seconds = estimate_render_allowance_seconds(
            max(countdown_seconds, required_countdown_seconds),
            has_music=has_music,
        )
        if next_allowance_seconds == render_allowance_seconds:
            break
        render_allowance_seconds = next_allowance_seconds

    countdown_start = ceil_to_representable_second(
        current_time + timedelta(seconds=render_allowance_seconds)
    )
    countdown_seconds = int((movie_start - countdown_start).total_seconds())
    if countdown_seconds < required_countdown_seconds:
        minimum_render_allowance = estimate_render_allowance_seconds(
            required_countdown_seconds,
            has_music=has_music,
        )
        soonest_movie_start = next_strict_second(
            current_time + timedelta(seconds=minimum_render_allowance + required_countdown_seconds)
        )
        if minimum_countdown_seconds > COUNTDOWN_MIN_SECONDS and not allow_music_truncation:
            raise SchedulerError(
                "The requested movie start does not leave enough time to include the full music "
                "track. "
                f"The music requires at least {format_countdown_mmss(minimum_countdown_seconds)}. "
                "With the current estimated render allowance, "
                f"the earliest movie-start time that would work is {soonest_movie_start:%Y-%m-%d %H:%M:%S}."
            )
        raise SchedulerError(
            "The requested movie start does not leave enough time for an automatically derived "
            "countdown. "
            "With the current estimated render allowance, "
            f"the earliest movie-start time that would work is {soonest_movie_start:%Y-%m-%d %H:%M:%S}. "
            f"The derived countdown would otherwise be shorter than "
            f"{format_countdown_mmss(required_countdown_seconds)}."
        )
    if countdown_seconds > COUNTDOWN_MAX_SECONDS:
        raise SchedulerError(
            "The requested movie start is too far away to derive an automatic countdown length. "
            "With the current estimated render allowance, "
            f"the derived countdown would be {format_countdown_mmss(countdown_seconds)}, which exceeds "
            f"the supported maximum of {format_countdown_mmss(COUNTDOWN_MAX_SECONDS)}. "
            "Specify --countdown-length explicitly instead."
        )
    return countdown_seconds, countdown_start


def resolve_movie_start(
    clock_time: dt_time,
    countdown_seconds: int,
    *,
    render_allowance_seconds: int = 0,
    now: datetime | None = None,
) -> datetime:
    current_time = now or datetime.now()
    movie_start = resolve_next_occurrence(clock_time, now=current_time)
    available_seconds = (movie_start - current_time).total_seconds()
    required_seconds = countdown_seconds + render_allowance_seconds
    if required_seconds >= available_seconds:
        soonest_movie_start = next_strict_second(
            current_time + timedelta(seconds=required_seconds)
        )
        raise SchedulerError(
            "The requested movie start does not leave enough time for the full countdown. "
            f"The countdown is currently set to {format_countdown_mmss(countdown_seconds)}. "
            f"The estimated render allowance is {render_allowance_seconds} seconds. "
            "When you ran the command, the earliest movie-start time that would have worked was "
            f"{soonest_movie_start:%Y-%m-%d %H:%M:%S}. "
            "Choose a later movie start, reuse an existing cached countdown, shorten the "
            "countdown, or use immediate mode."
        )
    return movie_start - timedelta(seconds=countdown_seconds)


def derive_countdown_seconds(music_path: Path | None, countdown_value: int | None) -> int:
    if countdown_value is not None:
        return countdown_value
    if music_path is None:
        return COUNTDOWN_DEFAULT_SECONDS

    buffered_duration = probe_media_duration_seconds(music_path) + 1
    derived = int(buffered_duration)
    if buffered_duration > derived:
        derived += 1
    if derived < COUNTDOWN_MIN_SECONDS:
        raise SchedulerError(
            "The derived countdown length from --music is shorter than the supported minimum "
            f"of {format_countdown_mmss(COUNTDOWN_MIN_SECONDS)}. Specify --countdown-length explicitly."
        )
    if derived > COUNTDOWN_MAX_SECONDS:
        raise SchedulerError(
            "The derived countdown length from --music exceeds the supported maximum "
            f"of {format_countdown_mmss(COUNTDOWN_MAX_SECONDS)}. Specify --countdown-length explicitly."
        )
    return derived


def validate_music_fits_countdown(
    music_path: Path | None,
    countdown_seconds: int,
    *,
    allow_music_truncation: bool = False,
) -> None:
    if music_path is None:
        return
    if allow_music_truncation:
        return

    minimum_music_countdown_seconds = derive_countdown_seconds(music_path, None)
    if countdown_seconds < minimum_music_countdown_seconds:
        raise SchedulerError(
            "The requested countdown is too short to include the full music track. "
            f"The music requires at least {format_countdown_mmss(minimum_music_countdown_seconds)}, "
            f"but the countdown is {format_countdown_mmss(countdown_seconds)}."
        )


def parse_resolution(
    value: str,
    *,
    source: str = "resolution",
    error_type: type[Exception] = argparse.ArgumentTypeError,
) -> VideoSize:
    match = RESOLUTION_PATTERN.fullmatch(value)
    if not match:
        raise error_type(
            f"{source} must be in WIDTHxHEIGHT format, for example {DEFAULT_RESOLUTION}."
        )
    return VideoSize(width=int(match.group("width")), height=int(match.group("height")))


def build_logger(path: Path) -> logging.Logger:
    path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.Logger(APP_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(path, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def resolve_display(display_name: str) -> Display:
    qInstallMessageHandler(lambda *_args: None)
    app = QGuiApplication.instance()
    if app is None:
        app = QGuiApplication([])
    assert isinstance(app, QGuiApplication)
    # Resolve once through Qt so VLC can be pointed at a stable display geometry.
    displays = [
        Display(
            index=index,
            name=screen.name(),
            x=screen.geometry().x(),
            y=screen.geometry().y(),
            width=screen.geometry().width(),
            height=screen.geometry().height(),
        )
        for index, screen in enumerate(app.screens())
    ]

    if not displays:
        raise SchedulerError("Qt did not report any displays.")

    for display in displays:
        if display.name == display_name:
            return display

    available = ", ".join(display.name for display in displays)
    raise SchedulerError(
        f"Configured fullscreen_display {display_name!r} was not found. "
        f"Available displays: {available}"
    )


def wait_until(target_epoch: float) -> None:
    # Sleep coarsely while far away, then tighten the polling window near the trigger time.
    while True:
        remaining = target_epoch - time.time()
        if remaining <= 0:
            return
        if remaining > 10:
            time.sleep(remaining - 5)
        elif remaining > 2:
            time.sleep(0.5)
        elif remaining > 0.5:
            time.sleep(0.05)
        else:
            time.sleep(0.01)


def format_countdown_mmss(total_seconds: int) -> str:
    return f"{total_seconds // 60:02d}:{total_seconds % 60:02d}"


def format_countdown_label(total_seconds: int) -> str:
    return f"{format_countdown_mmss(total_seconds)} countdown"


def format_countdown_filename_token(total_seconds: int) -> str:
    return f"{total_seconds // 60:02d}m{total_seconds % 60:02d}s"


def format_duration_precise(total_seconds: float) -> str:
    total_milliseconds = round(total_seconds * 1000)
    minutes, remainder = divmod(total_milliseconds, 60_000)
    seconds, milliseconds = divmod(remainder, 1000)
    return f"{minutes:02d}:{seconds:02d}.{milliseconds:03d}"


def build_music_cache_key(music_path: Path) -> str:
    stat_result = music_path.stat()
    fingerprint = (
        f"{music_path.resolve()}|{stat_result.st_size}|{stat_result.st_mtime_ns}".encode(
            "utf-8"
        )
    )
    return hashlib.sha256(fingerprint).hexdigest()[:12]


def resolve_font_path() -> Path:
    result = subprocess.run(
        [FC_MATCH_BINARY, "-f", "%{file}\n", FONT_FAMILY],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        text=True,
    )
    font_path = result.stdout.strip()
    if result.returncode != 0 or not font_path:
        raise SchedulerError(
            f"Could not resolve the required font via {FC_MATCH_BINARY}: {FONT_FAMILY}"
        )

    path = Path(font_path)
    if not path.is_file():
        raise SchedulerError(f"Resolved font path does not exist: {path}")
    return path


def probe_media_duration_seconds(media_path: Path) -> float:
    command = [
        FFPROBE_BINARY,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(media_path),
    ]
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise SchedulerError(
            f"Failed to probe music duration: {stderr or 'ffprobe returned a non-zero exit code.'}"
        )

    try:
        duration = float(result.stdout.strip())
    except ValueError as exc:
        raise SchedulerError("Failed to parse music duration from ffprobe output.") from exc

    if duration <= 0:
        raise SchedulerError(f"Music file duration must be positive: {media_path}")
    return duration


def probe_audio_stream_info(media_path: Path) -> AudioStreamInfo:
    command = [
        FFPROBE_BINARY,
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "stream=codec_name,sample_rate,channels,channel_layout",
        "-of",
        "json",
        str(media_path),
    ]
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise SchedulerError(
            f"Failed to probe music stream info: {stderr or 'ffprobe returned a non-zero exit code.'}"
        )

    try:
        payload = json.loads(result.stdout)
        stream = payload["streams"][0]
        codec_name = str(stream["codec_name"])
        sample_rate = int(stream["sample_rate"])
        channels = int(stream["channels"])
        channel_layout = stream.get("channel_layout")
    except (KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SchedulerError("Failed to parse music stream info from ffprobe output.") from exc

    return AudioStreamInfo(
        codec_name=codec_name,
        sample_rate=sample_rate,
        channels=channels,
        channel_layout=str(channel_layout) if channel_layout else None,
    )


def resolve_audio_channel_layout(audio_info: AudioStreamInfo) -> str:
    if audio_info.channel_layout:
        return audio_info.channel_layout

    fallback_layouts = {
        1: "mono",
        2: "stereo",
        3: "2.1",
        4: "4.0",
        5: "5.0",
        6: "5.1",
        7: "6.1",
        8: "7.1",
    }
    layout = fallback_layouts.get(audio_info.channels)
    if layout is None:
        raise SchedulerError(
            "Could not determine a channel layout for the music input. "
            f"Channel count was {audio_info.channels}."
        )
    return layout


def select_output_audio_encoder(audio_info: AudioStreamInfo) -> str:
    if audio_info.codec_name.startswith("pcm_"):
        return audio_info.codec_name
    return "pcm_s24le"


def build_countdown_audio_plan(
    music_path: Path,
    countdown_seconds: int,
    *,
    allow_music_truncation: bool,
) -> CountdownAudioPlan:
    audio_info = probe_audio_stream_info(music_path)
    music_duration = probe_media_duration_seconds(music_path)
    countdown_duration = float(countdown_seconds)
    trim_start = max(music_duration - countdown_duration, 0.0)
    start_delay = max(countdown_duration - music_duration, 0.0)
    if trim_start > 0 and not allow_music_truncation:
        raise SchedulerError(
            "Internal error: countdown generation would have trimmed the start of the music. "
            "This combination of arguments should have been rejected earlier."
        )
    return CountdownAudioPlan(
        music_duration=music_duration,
        trim_start=trim_start,
        start_delay=start_delay,
        encoder=select_output_audio_encoder(audio_info),
    )


def compute_render_plan(font_path: Path, resolution: VideoSize) -> CountdownRenderPlan:
    chars = "0123456789"
    scale = min(
        resolution.width / REFERENCE_WIDTH,
        resolution.height / REFERENCE_HEIGHT,
    )
    stroke_width = max(1, round(REFERENCE_TEXT_STROKE_WIDTH * scale))
    circle_left = round(
        resolution.width * (REFERENCE_CENTER_X - REFERENCE_OUTER_CIRCLE_RADIUS) / REFERENCE_WIDTH
    )
    circle_right = round(
        resolution.width * (REFERENCE_CENTER_X + REFERENCE_OUTER_CIRCLE_RADIUS) / REFERENCE_WIDTH
    )
    circle_gap = max(1, round(resolution.width * REFERENCE_CIRCLE_GAP_X / REFERENCE_WIDTH))
    side_margin_x = max(1, round(resolution.width * REFERENCE_SIDE_MARGIN_X / REFERENCE_WIDTH))
    side_margin_y = max(1, round(resolution.height * REFERENCE_SIDE_MARGIN_Y / REFERENCE_HEIGHT))
    lane_width = min(
        circle_left - circle_gap - side_margin_x,
        resolution.width - side_margin_x - (circle_right + circle_gap),
    )
    lane_height = resolution.height - 2 * side_margin_y
    if lane_width <= 0 or lane_height <= 0:
        raise SchedulerError("Configured countdown lanes do not fit inside the requested frame size.")

    def metrics_for_size(
        size: int,
    ) -> tuple[ImageFont.FreeTypeFont, dict[str, tuple[int, int, int, int]], int, int, int]:
        font = ImageFont.truetype(font_path, size)
        probe = ImageDraw.Draw(Image.new("RGB", (1, 1), "white"))
        advance = max(round(probe.textlength(ch, font=font)) for ch in chars)
        bboxes = {
            ch: normalize_bbox(
                probe.textbbox((0, 0), ch, font=font, anchor="ls", stroke_width=stroke_width)
            )
            for ch in chars
        }
        min_y = min(bbox[1] for bbox in bboxes.values())
        max_y = max(bbox[3] for bbox in bboxes.values())
        max_glyph_width = max(bbox[2] - bbox[0] for bbox in bboxes.values())
        slot_width = max(advance + 2 * stroke_width, max_glyph_width)
        slot_height = max_y - min_y
        return font, bboxes, min_y, slot_width, slot_height

    low, high = 1, 2000
    best_plan: CountdownRenderPlan | None = None
    while low <= high:
        mid = (low + high) // 2
        font, bboxes, min_y, slot_width, slot_height = metrics_for_size(mid)
        if slot_height <= lane_height and slot_width * 2 <= lane_width:
            left_x0 = round((side_margin_x + (circle_left - circle_gap) - (slot_width * 2)) / 2)
            right_x0 = round(
                ((circle_right + circle_gap) + (resolution.width - side_margin_x) - (slot_width * 2))
                / 2
            )
            y0 = round((resolution.height - slot_height) / 2)
            cell_cache: dict[str, Image.Image] = {}
            for ch, bbox in bboxes.items():
                glyph_width = bbox[2] - bbox[0]
                cell = Image.new("RGBA", (slot_width, slot_height), (0, 0, 0, 0))
                draw = ImageDraw.Draw(cell)
                # Each glyph is centered inside a fixed slot so timestamps never drift.
                anchor_x = round((slot_width - glyph_width) / 2) - bbox[0]
                anchor_y = -min_y
                draw.text(
                    (anchor_x, anchor_y),
                    ch,
                    font=font,
                    fill=TEXT_FILL,
                    anchor="ls",
                    stroke_width=stroke_width,
                    stroke_fill=TEXT_STROKE_FILL,
                )
                cell_cache[ch] = cell
            best_plan = CountdownRenderPlan(
                slot_width=slot_width,
                slot_height=slot_height,
                left_x0=left_x0,
                right_x0=right_x0,
                y0=y0,
                cell_cache=cell_cache,
            )
            low = mid + 1
        else:
            high = mid - 1

    if best_plan is None:
        raise SchedulerError("Could not fit the countdown text into the configured frame size.")
    return best_plan


def render_background_frame(
    background_path: Path,
    resolution: VideoSize,
    logger: logging.Logger,
) -> Image.Image:
    # Rasterize once and reuse the same background for every frame in this countdown.
    if not background_path.is_file():
        warning = (
            f"Countdown background file is missing: {background_path}. "
            "Using a plain black background instead."
        )
        logger.warning(warning)
        print(warning, file=sys.stderr)
        return Image.new("RGBA", (resolution.width, resolution.height), "black")

    command = [
        FFMPEG_BINARY,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(background_path),
        "-frames:v",
        "1",
        "-vf",
        f"scale={resolution.width}:{resolution.height}:flags=lanczos",
        "-f",
        "image2pipe",
        "-vcodec",
        "png",
        "-",
    ]
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise SchedulerError(
            "Failed to rasterize countdown background image: "
            f"{stderr or 'ffmpeg returned a non-zero exit code.'}"
        )

    try:
        image = Image.open(io.BytesIO(result.stdout))
        image.load()
    except Exception as exc:  # noqa: BLE001
        raise SchedulerError("Failed to decode the rasterized countdown background.") from exc
    return image.convert("RGBA")


def build_countdown_label_overlay(
    plan: CountdownRenderPlan,
    remaining_seconds: int,
) -> Image.Image:
    span_width = (plan.right_x0 + 2 * plan.slot_width) - plan.left_x0
    overlay = Image.new("RGBA", (span_width, plan.slot_height), (0, 0, 0, 0))
    minutes = f"{remaining_seconds // 60:02d}"
    seconds = f"{remaining_seconds % 60:02d}"
    # Fixed slot placement is what keeps the digits pixel-stable frame to frame.
    for index, ch in enumerate(minutes):
        overlay.alpha_composite(
            plan.cell_cache[ch],
            (index * plan.slot_width, 0),
        )
    right_offset = plan.right_x0 - plan.left_x0
    for index, ch in enumerate(seconds):
        overlay.alpha_composite(
            plan.cell_cache[ch],
            (right_offset + index * plan.slot_width, 0),
        )
    return overlay


def build_countdown_video(
    output_path: Path,
    countdown_seconds: int,
    resolution: VideoSize,
    background_path: Path,
    music_path: Path | None,
    *,
    allow_music_truncation: bool,
    logger: logging.Logger,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_video = output_path.with_name(f"{output_path.stem}.video.tmp{output_path.suffix}")
    tmp_output = output_path.with_name(f"{output_path.stem}.tmp{output_path.suffix}")
    tmp_video.unlink(missing_ok=True)
    if tmp_output.exists():
        tmp_output.unlink()

    font_path = resolve_font_path()
    plan = compute_render_plan(font_path, resolution)
    background = render_background_frame(background_path, resolution, logger)
    step_count = countdown_seconds + 1
    total_frames = step_count
    framerate = str(COUNTDOWN_TARGET_FPS)
    label_span_width = (plan.right_x0 + 2 * plan.slot_width) - plan.left_x0

    logger.info(
        "Rendering countdown video with %s over %s at %s fps (%s frames) to %s",
        FONT_FAMILY,
        background_path.name,
        framerate,
        total_frames,
        output_path,
    )
    with tempfile.TemporaryDirectory(prefix=f"{APP_NAME}_countdown_") as tmpdir_str:
        tmpdir = Path(tmpdir_str)
        background_frame = tmpdir / "background_frame.rgba"
        # Static background mode keeps the file cheap to generate: one rasterized frame
        # is looped for the whole countdown and only the per-second label overlay changes.
        background_frame.write_bytes(background.tobytes())

        label_cache: dict[int, bytes] = {}
        label_frames = list(range(countdown_seconds, -1, -1))
        for remaining_seconds in label_frames:
            label_cache[remaining_seconds] = build_countdown_label_overlay(
                plan,
                remaining_seconds,
            ).tobytes()

        command = [
            FFMPEG_BINARY,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-stream_loop",
            "-1",
            "-f",
            "rawvideo",
            "-pix_fmt",
            "rgba",
            "-s",
            resolution.text,
            "-framerate",
            framerate,
            "-i",
            str(background_frame),
            "-f",
            "rawvideo",
            "-pix_fmt",
            "rgba",
            "-s",
            f"{label_span_width}x{plan.slot_height}",
            "-framerate",
            framerate,
            "-i",
            "-",
            "-filter_complex",
            (
                f"[0:v]trim=duration={countdown_seconds:.6f},setpts=PTS-STARTPTS[bg];"
                f"[1:v]setpts=PTS-STARTPTS[fg];"
                f"[bg][fg]overlay={plan.left_x0}:{plan.y0}:format=auto,format=rgb24[out]"
            ),
            "-map",
            "[out]",
            "-an",
            "-c:v",
            COUNTDOWN_VIDEO_CODEC,
            "-preset",
            COUNTDOWN_VIDEO_PRESET,
            "-crf",
            COUNTDOWN_VIDEO_CRF,
            str(tmp_video),
        ]
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        try:
            assert process.stdin is not None
            for remaining_seconds in label_frames:
                process.stdin.write(label_cache[remaining_seconds])
            process.stdin.close()
            stderr = process.stderr.read().decode("utf-8", errors="replace") if process.stderr else ""
            returncode = process.wait()
        except BrokenPipeError:
            if process.stdin is not None:
                process.stdin.close()
            stderr = process.stderr.read().decode("utf-8", errors="replace") if process.stderr else ""
            returncode = process.wait()
        except Exception:
            process.kill()
            process.wait()
            raise

    if returncode != 0:
        tmp_video.unlink(missing_ok=True)
        tmp_output.unlink(missing_ok=True)
        raise SchedulerError(
            f"Countdown generation failed: {stderr.strip() or 'ffmpeg returned a non-zero exit code.'}"
        )

    if music_path is None:
        tmp_video.replace(output_path)
        return

    mux_countdown_audio(
        tmp_video,
        tmp_output,
        music_path,
        countdown_seconds,
        allow_music_truncation=allow_music_truncation,
        logger=logger,
    )
    tmp_video.unlink(missing_ok=True)
    tmp_output.replace(output_path)


def mux_countdown_audio(
    video_path: Path,
    output_path: Path,
    music_path: Path,
    countdown_seconds: int,
    *,
    allow_music_truncation: bool,
    logger: logging.Logger,
) -> None:
    audio_info = probe_audio_stream_info(music_path)
    audio_plan = build_countdown_audio_plan(
        music_path,
        countdown_seconds,
        allow_music_truncation=allow_music_truncation,
    )
    countdown_duration = float(countdown_seconds)
    channel_layout = resolve_audio_channel_layout(audio_info)
    # Countdown files with music always get PCM output so VLC only has to deal
    # with a simple uncompressed onset after the silent lead-in.
    encoder = audio_plan.encoder
    logger.info(
        "Muxing countdown audio from %s with trim_start=%.3fs start_delay=%.3fs encoder=%s",
        music_path,
        audio_plan.trim_start,
        audio_plan.start_delay,
        encoder,
    )

    music_filter_steps = []
    if audio_plan.trim_start > 0:
        music_filter_steps.append(f"atrim=start={audio_plan.trim_start:.6f}")
    music_filter_steps.extend(
        [
            "asetpts=PTS-STARTPTS",
            f"aformat=sample_rates={audio_info.sample_rate}:channel_layouts={channel_layout}",
        ]
    )

    filter_parts: list[str] = []
    if audio_plan.start_delay > 0:
        # Build real silent samples at t=0 so VLC never has to switch from an
        # initial gap into compressed audio partway through the file.
        filter_parts.append(
            f"anullsrc=r={audio_info.sample_rate}:cl={channel_layout}:d={audio_plan.start_delay:.6f}[silence]"
        )
        filter_parts.append(f"[1:a:0]{','.join(music_filter_steps)}[music]")
        filter_parts.append(
            "[silence][music]"
            f"concat=n=2:v=0:a=1,apad=whole_dur={countdown_duration:.6f},"
            f"atrim=0:{countdown_duration:.6f}[aout]"
        )
    else:
        filter_parts.append(
            f"[1:a:0]{','.join(music_filter_steps)},"
            f"apad=whole_dur={countdown_duration:.6f},"
            f"atrim=0:{countdown_duration:.6f}[aout]"
        )

    command = [
        FFMPEG_BINARY,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(video_path),
        "-i",
        str(music_path),
        "-filter_complex",
        ";".join(filter_parts),
        "-map",
        "0:v:0",
        "-map",
        "[aout]",
        "-c:v",
        "copy",
        "-c:a",
        encoder,
        "-ar",
        str(audio_info.sample_rate),
        "-ac",
        str(audio_info.channels),
        "-channel_layout",
        channel_layout,
        str(output_path),
    ]
    result = subprocess.run(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        output_path.unlink(missing_ok=True)
        stderr = result.stderr.strip()
        raise SchedulerError(
            f"Countdown audio mux failed: {stderr or 'ffmpeg returned a non-zero exit code.'}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate or reuse a cached countdown video, build a two-item VLC playlist, "
            "and either play it immediately or align it to a target local time."
        ),
        epilog=(
            "Examples:\n"
            "  %(prog)s -v /path/to/movie.mkv\n"
            "  %(prog)s -v /path/to/movie.mkv --countdown-start 17:55\n"
            "  %(prog)s -v /path/to/movie.mkv --movie-start 18:00 --countdown-length 05:00\n"
            "  %(prog)s -v /path/to/movie.mkv --movie-start 18:00 --music ./static/song.flac"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-v",
        "--video",
        required=True,
        type=parse_movie_path,
        metavar="FILE",
        help="Movie file to play after the countdown.",
    )
    scheduling_group = parser.add_argument_group(
        "Scheduling",
        "If you omit both options below, VLC is launched immediately and playback starts at once.",
    )
    start_group = scheduling_group.add_mutually_exclusive_group(required=False)
    start_group.add_argument(
        "--countdown-start",
        type=parse_clock_time,
        metavar="HH:MM[:SS]",
        help="Local time when the countdown should begin. Past times roll over to tomorrow.",
    )
    start_group.add_argument(
        "--movie-start",
        type=parse_clock_time,
        metavar="HH:MM[:SS]",
        help=(
            "Local time when the movie should begin. The countdown ends exactly at this time.\n"
            "If --countdown-length is omitted, the countdown length is derived from the remaining\n"
            "time minus a conservative render allowance that scales with countdown length.\n"
            f"The minimum reserved render window is {COUNTDOWN_RENDER_ALLOWANCE_SECONDS} seconds.\n"
            "If --music is also supplied, the full music track must still fit inside that derived\n"
            "countdown unless you pass --force."
        ),
    )
    countdown_group = parser.add_argument_group(
        "Countdown generation",
        "These options control the cached countdown asset that is created or reused.",
    )
    countdown_group.add_argument(
        "--countdown-length",
        type=parse_countdown_time,
        metavar="MM:SS",
        help=(
            "Countdown duration. Allowed range: 00:30 to 59:59.\n"
            "Default: 05:00. If --music is supplied in immediate mode, it becomes the music\n"
            "duration plus a 1 second buffer, rounded up. If --movie-start is supplied,\n"
            "the default becomes the remaining time until the movie minus the estimated render allowance."
        ),
    )
    countdown_group.add_argument(
        "--music",
        type=parse_music_path,
        metavar="FILE",
        help=(
            "Optional music file to embed in the countdown.\n"
            "The end of the music is aligned to the end of the countdown.\n"
            "Generated countdowns with music use PCM audio for robust playback.\n"
            "By default the script rejects countdowns that would trim the beginning of the music."
        ),
    )
    countdown_group.add_argument(
        "--force",
        action="store_true",
        help=(
            "Allow the start of the music to be truncated if the countdown is shorter than the track.\n"
            "Without this flag, such argument combinations are rejected."
        ),
    )
    countdown_group.add_argument(
        "--resolution",
        type=parse_resolution,
        metavar="WIDTHxHEIGHT",
        help="Countdown video resolution. Default comes from config.toml.",
    )
    countdown_group.add_argument(
        "--rebuild-countdown-cache",
        "--regenerate-countdown-video",
        dest="rebuild_countdown_cache",
        action="store_true",
        help="Regenerate the cached countdown file even if a matching cached version already exists.",
    )
    vlc_group = parser.add_argument_group(
        "VLC overrides",
        "These options override the defaults from config.toml for this run only.",
    )
    vlc_group.add_argument(
        "--subtitle-track",
        type=int,
        metavar="N",
        help="Subtitle track number to select for the movie entry.",
    )
    vlc_group.add_argument(
        "--display",
        "--screen",
        dest="display",
        metavar="NAME",
        help="Fullscreen display name, for example DP-1.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    config = Config.load(script_dir / "config.toml").with_overrides(
        fullscreen_display=args.display,
        subtitle_track=args.subtitle_track,
    )
    current_time = datetime.now()
    try:
        if args.movie_start is not None and args.countdown_length is None:
            minimum_countdown_seconds = (
                derive_countdown_seconds(args.music, None)
                if args.music is not None
                else COUNTDOWN_MIN_SECONDS
            )
            countdown_seconds, start_at = derive_movie_timed_countdown(
                args.movie_start,
                minimum_countdown_seconds=minimum_countdown_seconds,
                has_music=args.music is not None,
                allow_music_truncation=args.force,
                now=current_time,
            )
        else:
            countdown_seconds = derive_countdown_seconds(args.music, args.countdown_length)
            countdown_resolution = args.resolution or config.countdown_resolution
            countdown_path = resolve_countdown_cache_path(
                config,
                script_dir,
                countdown_seconds,
                countdown_resolution,
                args.music,
            )
            generation_required = args.rebuild_countdown_cache or not countdown_path.is_file()
            render_allowance_seconds = (
                estimate_render_allowance_seconds(
                    countdown_seconds,
                    has_music=args.music is not None,
                )
                if generation_required
                else 0
            )
            if args.countdown_start is not None:
                start_at = resolve_next_occurrence(args.countdown_start, now=current_time)
            elif args.movie_start is not None:
                start_at = resolve_movie_start(
                    args.movie_start,
                    countdown_seconds,
                    render_allowance_seconds=render_allowance_seconds,
                    now=current_time,
                )
            else:
                start_at = None
        validate_music_fits_countdown(
            args.music,
            countdown_seconds,
            allow_music_truncation=args.force,
        )
    except SchedulerError as exc:
        print(exc, file=sys.stderr)
        return 1

    scheduler = MovieStreamScheduler(
        config=config,
        inputs=Inputs(
            movie_path=args.video,
            start_at=start_at,
            countdown_seconds=countdown_seconds,
            countdown_resolution=args.resolution or config.countdown_resolution,
            music_path=args.music,
            regenerate_countdown_video=args.rebuild_countdown_cache,
            force_music_truncation=args.force,
        ),
        script_dir=script_dir,
    )

    try:
        return scheduler.run()
    except SchedulerError as exc:
        print(exc, file=sys.stderr)
        return 1
    finally:
        scheduler.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
