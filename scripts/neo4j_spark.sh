#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
NEO4J_DIR="$PROJECT_DIR/neo4j"
COMPOSE_FILE="$NEO4J_DIR/docker-compose.yml"

usage() {
  cat <<'EOF'
Manage Spark-local Neo4j container.

Usage:
  bash spark/scripts/neo4j_spark.sh <up|down|restart|status|logs>

Commands:
  up       Start Neo4j in detached mode
  down     Stop Neo4j
  restart  Restart Neo4j
  status   Show container status
  logs     Tail Neo4j logs
EOF
}

cmd="${1:-status}"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "[ERROR] Missing compose file: $COMPOSE_FILE"
  exit 1
fi

case "$cmd" in
  up)
    docker compose -f "$COMPOSE_FILE" up -d
    ;;
  down)
    docker compose -f "$COMPOSE_FILE" down
    ;;
  restart)
    docker compose -f "$COMPOSE_FILE" down
    docker compose -f "$COMPOSE_FILE" up -d
    ;;
  status)
    docker compose -f "$COMPOSE_FILE" ps
    ;;
  logs)
    docker compose -f "$COMPOSE_FILE" logs -f --tail=200 neo4j
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "[ERROR] Unknown command: $cmd"
    usage
    exit 1
    ;;
esac
