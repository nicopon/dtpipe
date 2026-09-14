#!/bin/bash
set -e

# validate_doc_links.sh
# Every relative link in the docs/ site resolves -- to a file that exists, and to a heading that
# exists in it.
#
# The site is navigation: twenty-two pages that cross-link each other about a hundred times, built
# precisely because a reader could not find things. A link to a renamed heading still renders, so
# nothing about writing or reviewing the page reveals it -- only following it does.
#
# Root documents are link targets here, not sources: REFERENCE.md and COOKBOOK.md are checked when
# a site page points into them, and their own links are left to their owners.
#
# To fix: correct the path, or the heading spelling the anchor is derived from.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Documentation Link Validation"
echo "========================================"

cd "$PROJECT_ROOT"

# Derived, never listed: every page of the site is a source of links, so a new page is covered by
# adding the file.
PAGES=()
while IFS= read -r page; do PAGES+=("$page"); done \
    < <(git ls-files --full-name -- 'docs/*.md' 'docs/**/*.md')

if [ ${#PAGES[@]} -eq 0 ]; then fail "no documentation page found under docs/"; fi

echo "  Resolving links across ${#PAGES[@]} pages..."

if problems="$(python3 "$SCRIPT_DIR/tools/check_doc_links.py" "${PAGES[@]}")"; then
    pass "every relative link and anchor resolves"
else
    echo "$problems" | sed 's/^/     /'
    fail "$(echo "$problems" | wc -l | tr -d ' ') broken link(s)"
fi

echo ""
echo -e "${GREEN}Documentation link validation complete!${NC}"
