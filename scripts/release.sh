#!/usr/bin/env bash
set -euo pipefail

# scripts/release.sh — cut a new Foreman release.
#
# Bumps the Version constant in config/config.go, commits the bump on
# main, tags v<version>, pushes, and creates a GitHub release with
# auto-generated notes. The build.yml workflow picks up the tag event
# and publishes ghcr.io/gaucho-racing/foreman:<version> (+ :{major},
# :{major}.{minor} via the semver metadata pattern).
#
#   ./scripts/release.sh 0.2.0
#   ./scripts/release.sh          # prompts for version
#
# Preflight: must be on main, clean working tree, up to date with
# origin/main, the new tag doesn't already exist, and gh + git are
# installed. Bails loudly on any of those.

usage() {
    cat <<EOF
Usage: $0 [<version>]

Examples:
  $0                 # prompt for version
  $0 0.2.0           # cut v0.2.0
EOF
}

while getopts ":h" opt; do
    case $opt in
        h) usage; exit 0 ;;
        *) usage; exit 1 ;;
    esac
done
shift $((OPTIND - 1))

# Required tools.
for cmd in gh git; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: $cmd is required"
        exit 1
    fi
done

# Must be on main.
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$BRANCH" != "main" ]]; then
    echo "Error: must be on main branch (currently on $BRANCH)"
    exit 1
fi

# Local main must match origin/main.
git fetch origin main --tags --quiet
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)
if [[ "$LOCAL" != "$REMOTE" ]]; then
    echo "Error: local main is not up to date with origin/main"
    echo "  local:  $LOCAL"
    echo "  remote: $REMOTE"
    exit 1
fi

# Working tree must be clean — we're going to add config/config.go and
# don't want to accidentally include unrelated edits.
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Error: working tree has uncommitted changes"
    git status -s
    exit 1
fi

# Resolve previous tag for display.
PREV=$(git tag -l 'v*' | sort -V | tail -n1)

INPUT="${1:-}"
if [[ -z "$INPUT" ]]; then
    echo
    if [[ -n "$PREV" ]]; then
        echo "Current release: ${PREV}"
    else
        echo "Current release: (none)"
    fi
    echo
    read -rp "Enter new version: " INPUT
fi

if [[ -z "$INPUT" ]]; then
    echo "Error: version cannot be empty"
    exit 1
fi
INPUT="${INPUT#v}"
if [[ ! "$INPUT" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: version must be a valid semver (e.g. 0.2.0)"
    exit 1
fi
VERSION="v${INPUT}"
TAG="$VERSION"

if git tag -l "$TAG" | grep -q "^${TAG}$"; then
    echo "Error: tag $TAG already exists"
    exit 1
fi

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

# Show summary + confirm.
echo
echo "=== Release Summary ==="
echo "  Version: ${VERSION}"
echo "  Tag:     ${TAG}"
echo "  Commit:  $(git rev-parse --short HEAD)"
echo "  Branch:  main"
echo
echo "  Files to update:"
echo "    config/config.go (Version)"
echo
echo "  Docker images that will be tagged:"
echo "    ghcr.io/gaucho-racing/foreman:${INPUT}"
MAJOR_MINOR=$(echo "$INPUT" | awk -F. '{print $1"."$2}')
MAJOR=$(echo "$INPUT" | awk -F. '{print $1}')
echo "    ghcr.io/gaucho-racing/foreman:${MAJOR_MINOR}"
echo "    ghcr.io/gaucho-racing/foreman:${MAJOR}"
echo
read -rp "Proceed? (y/N) " CONFIRM
if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

# Bump the Version constant. macOS sed requires an explicit '' after -i;
# Linux sed treats it as harmless. The replacement matches the literal
# `Version: "..."` form used in config/config.go.
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s/Version: \"[^\"]*\"/Version: \"${INPUT}\"/" config/config.go
else
    sed -i "s/Version: \"[^\"]*\"/Version: \"${INPUT}\"/" config/config.go
fi

git add config/config.go
git commit -m "release: foreman ${VERSION}"
git push origin main

# Cut the release. --generate-notes pulls the commit history since the
# previous tag into the body. --target main pins it to the commit we
# just pushed so the tag lands on the right SHA even if main moved
# while gh was waking up.
gh release create "$TAG" \
    --target main \
    --title "${VERSION}" \
    --generate-notes

echo
echo "Done. ${TAG} released. The build workflow will publish the image to ghcr.io shortly."
