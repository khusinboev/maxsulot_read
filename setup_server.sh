#!/usr/bin/env bash
# Ubuntu server bootstrap for this repository layout.

set -euo pipefail

# Loyiha serverda /home/maxs ichida joylashgan.
# Kerak bo'lsa ishga tushirishda ROOT_DIR env bilan override qilish mumkin.
ROOT_DIR="${ROOT_DIR:-/home/maxs}"
VENV_DIR="$ROOT_DIR/.venv"
SESSION_NAME="trofey"
TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-8190250269:AAEnmkUac-MQGQ_tC_n7o2WN-iCNXduVNos}"
TELEGRAM_USER_ID="${TELEGRAM_USER_ID:-1918760732}"
TELEGRAM_BATCH_SIZE="${TELEGRAM_BATCH_SIZE:-100}"

if [ ! -f "$ROOT_DIR/requirements.txt" ]; then
    echo "ERR: $ROOT_DIR/requirements.txt topilmadi"
    echo "ROOT_DIR ni tekshiring yoki vaqtincha ROOT_DIR=/to/gri/path bash setup_server.sh deb ishga tushiring."
    exit 1
fi

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
echo "Telegram user: $TELEGRAM_USER_ID"
echo "Telegram batch size: $TELEGRAM_BATCH_SIZE"

cat > "$ROOT_DIR/.runtime.env" <<EOF
export TELEGRAM_BOT_TOKEN='$TELEGRAM_BOT_TOKEN'
export TELEGRAM_USER_ID='$TELEGRAM_USER_ID'
export TELEGRAM_BATCH_SIZE='$TELEGRAM_BATCH_SIZE'
EOF

chmod 600 "$ROOT_DIR/.runtime.env"

echo "================================================"
echo "  RUN COMMANDS"
echo "================================================"
echo "source $VENV_DIR/bin/activate"
echo "source $ROOT_DIR/.runtime.env"
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
echo "source $ROOT_DIR/.runtime.env"
echo "python $ROOT_DIR/step/step6_run.py --notify-telegram"
echo "# detach: Ctrl+A, D"

echo
echo "# screen ichiga kirmasdan birdaniga ishga tushirish:"
echo "screen -dmS $SESSION_NAME bash -lc 'source $VENV_DIR/bin/activate; source $ROOT_DIR/.runtime.env; python $ROOT_DIR/step/step6_run.py --notify-telegram'"
