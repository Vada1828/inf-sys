#!/bin/sh
set -e

echo "=== MIGRATIONS TR ==="
flask --app app.py db init --directory migrations_tr || true
flask --app app.py db migrate --directory migrations_tr || true
flask --app app.py db upgrade --directory migrations_tr

echo "=== MIGRATIONS WH ==="
flask --app app2.py db init --directory migrations_wh || true
flask --app app2.py db migrate --directory migrations_wh || true
flask --app app2.py db upgrade --directory migrations_wh

echo "=== STARTING BOTH ==="
python app.py & python app2.py
