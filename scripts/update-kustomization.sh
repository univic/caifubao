#!/bin/bash
# update-kustomization.sh - 更新 kustomization.yaml 中的镜像标签

set -e

# 参数检查
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

# 创建临时文件
TMP_FILE=$(mktemp)

# 使用 awk 处理多行替换
awk -v img="$IMAGE_NAME" -v tag="$IMAGE_TAG" '
BEGIN { found = 0 }
/^- name: / {
    if (found) {
        print "  newTag: " tag
        found = 0
    }
    if ($0 ~ "^- name: " img "$") {
        found = 1
    }
}
{ print }
END {
    if (found) {
        print "  newTag: " tag
    }
}
' "$KUSTOMIZATION_FILE" > "$TMP_FILE"

# 替换原文件
mv "$TMP_FILE" "$KUSTOMIZATION_FILE"

echo "Updated kustomization.yaml:"
cat "$KUSTOMIZATION_FILE"
