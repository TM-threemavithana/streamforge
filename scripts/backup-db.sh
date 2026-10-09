#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${1:-docker-postgres-1}"
DB_USER="${2:-streamforge}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BACKUP_DIR="${REPO_ROOT}/.tmp/backups"

mkdir -p "$BACKUP_DIR"

echo "[*] Backing up StreamForge Core database..."
docker exec "$CONTAINER" pg_dump -U "$DB_USER" streamforge > "${BACKUP_DIR}/streamforge_core_${TIMESTAMP}.sql"
echo "[+] Core backup created: ${BACKUP_DIR}/streamforge_core_${TIMESTAMP}.sql"

echo "[*] Backing up StreamForge Alerts database..."
docker exec "$CONTAINER" pg_dump -U "$DB_USER" streamforge_alerts > "${BACKUP_DIR}/streamforge_alerts_${TIMESTAMP}.sql"
echo "[+] Alerts backup created: ${BACKUP_DIR}/streamforge_alerts_${TIMESTAMP}.sql"

echo "[+] Backups completed successfully."
