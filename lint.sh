#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

VENV_BIN="${HOME}/.virtualenvs/uvs_movie_streaming/bin"
PYTHON_BIN="${VENV_BIN}/python3"
FLAKE8_BIN="${VENV_BIN}/flake8"
PYRIGHT_BIN="${VENV_BIN}/pyright"

if [ ! -x "${PYTHON_BIN}" ]; then
    echo "uvs_movie_streaming virtualenv not found at ${VENV_BIN}" >&2
    exit 1
fi

"${PYTHON_BIN}" -m py_compile schedule_uvs_movie_stream.py

if [ -x "${FLAKE8_BIN}" ]; then
    "${FLAKE8_BIN}" schedule_uvs_movie_stream.py
else
    echo "flake8 not installed in ${VENV_BIN}; skipping"
fi

if [ -x "${PYRIGHT_BIN}" ]; then
    "${PYRIGHT_BIN}" schedule_uvs_movie_stream.py
else
    echo "pyright not installed in ${VENV_BIN}; skipping"
fi
