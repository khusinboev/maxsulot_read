#!/usr/bin/env bash
# Ubuntu server bootstrap for this repository layout.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
SESSION_NAME="trofey"

echo "================================================"
echo "  TROFEY PIPELINE SETUP"
echo "================================================"
echo "Project dir: $ROOT_DIR"

echo "[1/4] Python venv yaratilmoqda..."
python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

echo "[2/4] Dependencylar o'rnatilmoqda..."
python -m pip install --upgrade pip
python -m pip install -r "$ROOT_DIR/requirements.txt"

echo "[3/4] Asosiy fayllar tekshirilmoqda..."
for f in "$ROOT_DIR/brands.json" "$ROOT_DIR/step/run.py" "$ROOT_DIR/step/step6_run.py" "$ROOT_DIR/step/step6_scraper.py"; do
    if [ -f "$f" ]; then
        echo "  OK  $f"
    else
        echo "  ERR $f topilmadi"
        exit 1
    fi
done

echo "[4/4] Tayyor."
echo
echo "================================================"
echo "  RUN COMMANDS"
echo "================================================"
echo "source $VENV_DIR/bin/activate"
echo
echo "# 1) To'liq pipeline (prepare+search+filter):"
echo "python $ROOT_DIR/step/run.py --all"
echo
echo "# 2) Faqat scraping (clean_products dan):"
echo "python $ROOT_DIR/step/step6_run.py --no-images"
echo
echo "# 3) Holat:"
echo "python $ROOT_DIR/step/step6_run.py --stats"
echo
echo "# screen bilan fon rejim:"
echo "screen -S $SESSION_NAME"
echo "source $VENV_DIR/bin/activate"
echo "python $ROOT_DIR/step/step6_run.py --no-images"
echo "# detach: Ctrl+A, D"
