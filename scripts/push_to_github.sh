#!/bin/bash
# Push to GitHub script
# Usage: ./scripts/push_to_github.sh <GITHUB_REPO_URL>

set -e

REPO_URL="${1:-}"

if [ -z "$REPO_URL" ]; then
    echo "Usage: $0 <GITHUB_REPO_URL>"
    echo ""
    echo "Example:"
    echo "  $0 https://github.com/your-org/pit-feature-store.git"
    echo "  $0 git@github.com:your-org/pit-feature-store.git"
    echo ""
    exit 1
fi

echo "Adding remote origin..."
git remote add origin "$REPO_URL" 2>/dev/null || git remote set-url origin "$REPO_URL"

echo "Pushing to GitHub..."
git push -u origin HEAD

echo ""
echo "✓ Successfully pushed to GitHub!"
echo "Repository: $REPO_URL"
