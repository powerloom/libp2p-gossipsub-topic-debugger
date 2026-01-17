#!/bin/bash

# Bootstrap script for libp2p-gossipsub-topic-debugger
# Clones relayer-py repository for building from source (development mode)
# If ./relayer-py already exists, skips cloning
# If you want to use pre-built image instead, just don't run this script
# and docker-compose will pull from registry (production mode)

set -e

# Load .env file if it exists (for RELAYER_PY_BRANCH and other config)
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_color() {
    color=$1
    shift
    echo -e "${color}$@${NC}"
}

RELAYER_REPO="https://github.com/powerloom/relayer-py.git"
RELAYER_DIR="./relayer-py"

if [ -d "$RELAYER_DIR" ]; then
    print_color "$GREEN" "✅ relayer-py directory already exists at $RELAYER_DIR"
    print_color "$CYAN" "💡 To use a different version, remove the directory and run bootstrap again"
    exit 0
fi

print_color "$CYAN" "📦 Cloning relayer-py repository for development build..."
print_color "$YELLOW" "Repository: $RELAYER_REPO"
print_color "$YELLOW" "Target directory: $RELAYER_DIR"
print_color "$YELLOW" ""
print_color "$YELLOW" "Note: If you prefer to use a pre-built image, skip this step"
print_color "$YELLOW" "and docker-compose will pull from registry instead"

if ! git clone "$RELAYER_REPO" "$RELAYER_DIR"; then
    print_color "$RED" "❌ Failed to clone relayer-py repository"
    print_color "$YELLOW" ""
    print_color "$YELLOW" "You can still use docker-compose - it will pull pre-built image from registry"
    exit 1
fi

print_color "$GREEN" "✅ Successfully cloned relayer-py"

# Switch to specified branch (from .env) or default to master
RELAYER_BRANCH="${RELAYER_PY_BRANCH:-master}"
print_color "$CYAN" "🔄 Switching to branch: $RELAYER_BRANCH"
if [ -n "$RELAYER_PY_BRANCH" ]; then
    print_color "$CYAN" "   (from RELAYER_PY_BRANCH in .env)"
fi
if ! (cd "$RELAYER_DIR" && git checkout "$RELAYER_BRANCH" 2>/dev/null); then
    print_color "$YELLOW" "⚠️  Branch $RELAYER_BRANCH not found, using default branch"
else
    print_color "$GREEN" "✅ Switched to branch: $RELAYER_BRANCH"
fi

print_color "$GREEN" "✅ Bootstrap complete!"
print_color "$CYAN" ""
print_color "$CYAN" "Next steps:"
print_color "$CYAN" "  1. Configure VPA_SIGNER_ADDRESSES and VPA_SIGNER_PRIVATE_KEYS in .env"
print_color "$CYAN" "  2. Run: docker-compose up -d"
print_color "$CYAN" ""
print_color "$CYAN" "Docker-compose will:"
print_color "$CYAN" "  - Build from source if ./relayer-py exists (development)"
print_color "$CYAN" "  - Pull from registry if ./relayer-py doesn't exist (production)"
