#!/bin/sh
# ================================================================
# Grafana Custom Entrypoint
# Auto-provisions BigQuery datasource from GCP service account JSON
# ================================================================
set -e

SA_FILE="/opt/gcp/service-account.json"
DS_DIR="/etc/grafana/provisioning/datasources"
DS_FILE="$DS_DIR/bigquery.yml"

echo "=========================================="
echo "[Grafana Init] Starting auto-provisioning..."
echo "=========================================="

if [ -f "$SA_FILE" ]; then
  PROJECT_ID=$(jq -r '.project_id' "$SA_FILE")
  CLIENT_EMAIL=$(jq -r '.client_email' "$SA_FILE")

  echo "[Grafana Init] Detected GCP project: $PROJECT_ID"
  echo "[Grafana Init] Service account: $CLIENT_EMAIL"

  # Generate BigQuery datasource YAML with credentials from SA JSON
  # The private key is multi-line PEM — we use YAML block scalar (|) with proper indentation
  {
    echo "apiVersion: 1"
    echo "datasources:"
    echo "  - name: BigQuery"
    echo "    uid: bigquery"
    echo "    type: grafana-bigquery-datasource"
    echo "    access: proxy"
    echo "    isDefault: true"
    echo "    editable: true"
    echo "    jsonData:"
    echo "      authenticationType: jwt"
    echo "      clientEmail: \"${CLIENT_EMAIL}\""
    echo "      defaultProject: \"${PROJECT_ID}\""
    echo "      tokenUri: https://oauth2.googleapis.com/token"
    echo "    secureJsonData:"
    echo "      privateKey: |"
    jq -r '.private_key' "$SA_FILE" | sed 's/^/        /'
  } > "$DS_FILE"

  echo "[Grafana Init] BigQuery datasource provisioned at: $DS_FILE"
else
  echo "[Grafana Init] WARNING: No service account found at $SA_FILE"
  echo "[Grafana Init] BigQuery datasource will NOT be auto-configured."
  echo "[Grafana Init] You can manually configure it via Grafana UI."
fi

echo "[Grafana Init] Auto-provisioning complete. Starting Grafana server..."
echo "=========================================="

# Delegate to official Grafana entrypoint
exec /run.sh
