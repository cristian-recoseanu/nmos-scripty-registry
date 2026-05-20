#!/bin/bash
set -euo pipefail

# Build Docker image and save as tar
# Usage: ./build-and-save-image.sh [IMAGE_NAME] [OUTPUT_TAR] [TAG1] [TAG2] ...

IMAGE_NAME="${1:-nmos-scripty-registry}"
OUTPUT_TAR="${2:-${IMAGE_NAME}.tar}"
shift 2
TAGS=("$@")

echo "Building Docker image: ${IMAGE_NAME}"
docker build -t "${IMAGE_NAME}" .

# Apply additional tags if provided
for tag in "${TAGS[@]}"; do
  echo "Tagging image: ${IMAGE_NAME}:${tag}"
  docker tag "${IMAGE_NAME}" "${IMAGE_NAME}:${tag}"
done

echo "Saving image to tar: ${OUTPUT_TAR}"
docker save "${IMAGE_NAME}" -o "${OUTPUT_TAR}"

echo "Done: ${OUTPUT_TAR}"
