#!/bin/bash
# update-kustomization.sh - set an overlay's image tag in kustomization.yaml
#
# Idempotent: the existing `newTag` inside the matching image block is replaced
# (not appended), so running the script twice cannot produce duplicate keys.

set -euo pipefail

if [ $# -lt 3 ]; then
    echo "Usage: $0 <overlay-dir> <image-name> <image-tag>"
    echo "Example: $0 k8s/overlays/example-development caifubao-backend develop"
    exit 1
fi

OVERLAY_DIR="$1"
IMAGE_NAME="$2"
IMAGE_TAG="$3"

echo "Updating ${IMAGE_NAME} image tag to: ${IMAGE_TAG}"
echo "Overlay directory: ${OVERLAY_DIR}"

KUSTOMIZATION_FILE="${OVERLAY_DIR}/kustomization.yaml"

if [ ! -f "$KUSTOMIZATION_FILE" ]; then
    echo "Error: ${KUSTOMIZATION_FILE} not found"
    exit 1
fi

python3 - "$KUSTOMIZATION_FILE" "$IMAGE_NAME" "$IMAGE_TAG" <<'PY'
import re
import sys

path, image_name, image_tag = sys.argv[1:4]
with open(path, encoding="utf-8") as handle:
    text = handle.read()

pattern = re.compile(
    rf"(?ms)^- name: {re.escape(image_name)}\n(?P<body>(?:  [^\n]*\n?)+)"
)
match = pattern.search(text)
if match is None:
    sys.exit(f"image {image_name!r} not found in {path}")

body = re.sub(r"(?m)^  newTag:.*\n?", "", match.group("body"))
if re.search(r"(?m)^  newName:", body):
    body = re.sub(
        r"(?m)^(  newName:.*\n)", r"\1" + f"  newTag: {image_tag}\n", body, count=1
    )
else:
    body = body.rstrip("\n") + f"\n  newTag: {image_tag}\n"

text = text[: match.start()] + f"- name: {image_name}\n" + body + text[match.end():]
with open(path, "w", encoding="utf-8") as handle:
    handle.write(text)
PY

echo "Updated ${KUSTOMIZATION_FILE}:"
cat "$KUSTOMIZATION_FILE"
