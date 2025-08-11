# Telegram Username Hunter

This repo provides two bots: account manager (add.py) and username hunter (User_check.py). It supports preview, local filtering, smart priority, runtime controls, improved claiming, persistence/resume, reports, multi-task, auto-alerts, and optional distributed queues.

## Quick start (Termux / Android)

1) Install Python and dependencies:

```
pkg update && pkg upgrade -y
pkg install -y python git openssl clang 
# optional: libffi libcrypt for crypto acceleration

# Clone your repo
cd ~
git clone <your-repo-url> hunter && cd hunter

# Install requirements (light set)
pip install --upgrade pip
pip install -r requirements.txt
```

2) Set environment (optional; defaults are embedded in code):

```
export TG_API_ID=26924046
export TG_API_HASH='4c6ef4cee5e129b7a674de156e2bcc15'
export BOT_TOKEN='7941972743:AAFMmZgx2gRBgOaiY4obfhawleO9p1_TYn8'
export ADMIN_IDS='985612253'
export ENCRYPTION_SALT='default_salt'
export ENCRYPTION_PASSPHRASE='default_pass'
export DB_PATH='accounts.db'
# Optional distributed mode
export DISTRIBUTED_MODE=0
```

3) Run the hunter bot:

```
python User_check.py
```

Use /start in Telegram to choose target, category, and template.

## Deploy on Render

This repo includes:
- `Procfile` (worker)
- `render.yaml` (blueprint)

Steps:
1) Push to GitHub.
2) In Render, New > Blueprint, select your repo. Render will detect `render.yaml`.
3) It will auto-provision a Python worker and install `requirements_enhanced.txt`, then run `python User_check.py`.

Environment variables are set in `render.yaml` with current code defaults:
- TG_API_ID: 26924046
- TG_API_HASH: 4c6ef4cee5e129b7a674de156e2bcc15
- BOT_TOKEN: 7941972743:AAFMmZgx2gRBgOaiY4obfhawleO9p1_TYn8
- ADMIN_IDS: 985612253
- ENCRYPTION_SALT: default_salt
- ENCRYPTION_PASSPHRASE: default_pass
- DB_PATH: accounts.db
- DISTRIBUTED_MODE: 0

Please rotate these in production and use Render Dashboard to override.

## Notes
- For heavy builds (numpy/pandas) use `requirements_enhanced.txt` on servers with build tools.
- Ensure `accounts.db` is present with accounts (via `add.py`).
- For multi-instance work, share the same DB and set `DISTRIBUTED_MODE=1`.