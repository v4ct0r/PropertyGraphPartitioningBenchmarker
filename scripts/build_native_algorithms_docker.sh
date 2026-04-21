#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_DIR="$(cd "$PROJECT_DIR/.." && pwd)"

IMAGE_TAG="property-graph-native-partitioners:latest"
CONTEXT_DIR=""

usage() {
  cat <<'EOF'
Build a pinned Docker image containing KaHIP, METIS, ParMETIS, and SCOTCH built from local source trees.

Usage:
  bash scripts/build_native_algorithms_docker.sh [options]

Options:
  --image-tag <tag>   Docker image tag (default: property-graph-native-partitioners:latest)
  --context-dir <p>   Build context directory (default: temporary directory under /tmp)
  --help              Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image-tag)
      IMAGE_TAG="${2:?missing value for --image-tag}"
      shift 2
      ;;
    --context-dir)
      CONTEXT_DIR="${2:?missing value for --context-dir}"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

pick_source_dir() {
  local preferred="$1"
  local fallback="$2"
  if [[ -d "$preferred" ]]; then
    printf '%s\n' "$preferred"
  else
    printf '%s\n' "$fallback"
  fi
}

KAHIP_SRC="$(pick_source_dir "$PROJECT_DIR/algorithms/KaHIP" "$WORKSPACE_DIR/KaHIP")"
METIS_SRC="$(pick_source_dir "$PROJECT_DIR/algorithms/METIS" "$WORKSPACE_DIR/METIS")"
GKLIB_SRC="$(pick_source_dir "$PROJECT_DIR/algorithms/GKlib" "$WORKSPACE_DIR/GKlib")"
SCOTCH_SRC="$PROJECT_DIR/algorithms/scotch-7.0.4"
PARMETIS_SRC="$PROJECT_DIR/algorithms/ParMETIS"
DOCKERFILE_SRC="$PROJECT_DIR/docker/native-algorithms/Dockerfile"

for path in "$KAHIP_SRC" "$METIS_SRC" "$GKLIB_SRC" "$PARMETIS_SRC" "$SCOTCH_SRC" "$DOCKERFILE_SRC"; do
  if [[ ! -e "$path" ]]; then
    echo "[ERROR] Required source path not found: $path"
    exit 1
  fi
done

if [[ -z "$CONTEXT_DIR" ]]; then
  CONTEXT_DIR="$(mktemp -d /tmp/property-graph-native-build.XXXXXX)"
  trap 'rm -rf "$CONTEXT_DIR"' EXIT
else
  mkdir -p "$CONTEXT_DIR"
  rm -rf "$CONTEXT_DIR"/KaHIP "$CONTEXT_DIR"/METIS "$CONTEXT_DIR"/GKlib "$CONTEXT_DIR"/ParMETIS "$CONTEXT_DIR"/scotch-7.0.4
fi

cp "$DOCKERFILE_SRC" "$CONTEXT_DIR/Dockerfile"
cp -a "$KAHIP_SRC" "$CONTEXT_DIR/KaHIP"
cp -a "$METIS_SRC" "$CONTEXT_DIR/METIS"
cp -a "$GKLIB_SRC" "$CONTEXT_DIR/GKlib"
cp -a "$PARMETIS_SRC" "$CONTEXT_DIR/ParMETIS"
cp -a "$SCOTCH_SRC" "$CONTEXT_DIR/scotch-7.0.4"

echo "[INFO] Building Docker image: $IMAGE_TAG"
docker build -t "$IMAGE_TAG" "$CONTEXT_DIR"
echo "[OK] Built image: $IMAGE_TAG"
