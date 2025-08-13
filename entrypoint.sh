#!/usr/bin/env sh
set -euo pipefail

# start genomes server in background
python3 /app/src/app/genome_database/serve_published_genomes.py &
GENOMES_PID=$!

# ensure we stop the background server on exit
term_handler() {
  kill -TERM "$GENOMES_PID" 2>/dev/null || true
  wait "$GENOMES_PID" 2>/dev/null || true
}
trap term_handler INT TERM

# start Streamlit in foreground (replace with your main app command)
exec /app/venv/bin/streamlit run /app/src/app/app.py --server.address 0.0.0.0 --server.port 8501
