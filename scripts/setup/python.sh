#!/usr/bin/env bash
#
# Copyright 2025 coze-dev Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


SETUP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_DIR="$(dirname "$SETUP_DIR")"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
BACKEND_DIR="$BASE_DIR/backend"
BIN_DIR="$BASE_DIR/bin"
VENV_DIR="$BIN_DIR/.venv"

echo "Checking for Python virtual environment under $BIN_DIR"

if [ ! -f "$VENV_DIR/bin/activate" ]; then
    echo "Virtual environment not found or incomplete. Re-creating..."
    rm -rf "$VENV_DIR"
    python3 -m venv "$VENV_DIR"

    if [ $? -ne 0 ]; then
        echo "Failed to create virtual environment - aborting startup"
        exit 1
    fi
    echo "Virtual environment created successfully!"
else
    echo "Virtual environment already exists. Skipping creation."
fi


echo "Installing required Python packages"
source "$VENV_DIR/bin/activate"

# Unset proxy variables to avoid SOCKS support issues
unset all_proxy http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

pip install --upgrade pip
# If you want to use other third-party libraries, you can install them here.
pip install urllib3==1.26.16
pip install h11==0.16.0 httpx==0.28.1 pillow==11.2.1 pdfplumber==0.11.7 python-docx==1.2.0 numpy==2.2.6

if [ $? -ne 0 ]; then
    echo "Failed to install Python packages - aborting startup"
    deactivate
    exit 1
fi

echo "Python packages installed successfully!"
deactivate

PARSER_SCRIPT_ROOT="$BACKEND_DIR/infra/impl/document/parser/builtin"
PDF_PARSER="$PARSER_SCRIPT_ROOT/parse_pdf.py"
DOCX_PARSER="$PARSER_SCRIPT_ROOT/parse_docx.py"
WORKFLOW_SANBOX="$BACKEND_DIR/infra/impl/coderunner/script/sandbox.py"

if [ -f "$PDF_PARSER" ]; then
    cp "$PDF_PARSER" "$BIN_DIR/parse_pdf.py"
else
    echo "❌ $PDF_PARSER file not found"
    exit 1
fi

if [ -f "$DOCX_PARSER" ]; then
    cp "$DOCX_PARSER" "$BIN_DIR/parse_docx.py"
else
    echo "❌ $DOCX_PARSER file not found"
    exit 1
fi

if [ -f "$WORKFLOW_SANBOX" ]; then
    cp "$WORKFLOW_SANBOX" "$BIN_DIR/sandbox.py"
else
    echo "❌ $WORKFLOW_SANBOX file not found"
    exit 1
fi