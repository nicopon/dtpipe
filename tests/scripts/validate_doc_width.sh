#!/bin/bash
set -e

# validate_doc_width.sh
# Documentation prose stays within a width a human can scan.
#
# The three user-facing documents had drifted to paragraphs written as a single physical line:
# 23 lines over 200 characters in REFERENCE.md alone, the longest 2 299 — one paragraph carrying
# the whole behaviour of the agent's scrollback output, with no line break in it. In a rendered
# view that is a wall of text; in an editor it is one line folded over ten screens, and a diff
# on it shows the paragraph, never the sentence that changed.
#
# The check asks the formatter rather than counting characters: a line is reported only if
# tests/scripts/tools/reflow_markdown.py would actually shorten it. A line whose overflow is one
# unbreakable atom — a long URL, a link, an inline code span — is left alone, since no line break
# makes it shorter. Code fences, tables and headings are never touched.
#
# To fix: tests/scripts/tools/reflow_markdown.py --write <files>
#
# CLAUDE.md is deliberately outside the perimeter: it is the contributor/model brief, not a
# document a user of dtpipe reads, and rewrapping it is its owner's call.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WIDTH=100

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Documentation Width Validation"
echo "========================================"

cd "$PROJECT_ROOT"

# Derived, never listed: every tracked Markdown file at the repository root is user-facing
# documentation, so a new one is covered by adding the file.
DOCS=()
while IFS= read -r doc; do DOCS+=("$doc"); done \
    < <(git ls-files --full-name -- '*.md' | grep -v / | grep -v '^CLAUDE.md$')
if [ ${#DOCS[@]} -eq 0 ]; then fail "no documentation file found at the repository root"; fi

echo "  Checking ${DOCS[*]} at $WIDTH columns..."

if offenders="$(python3 "$SCRIPT_DIR/tools/reflow_markdown.py" --width "$WIDTH" "${DOCS[@]}")"; then
    pass "${#DOCS[@]} documents within $WIDTH columns"
else
    echo "$offenders" | sed 's/^/     /'
    echo -e "  ${RED}Fix with: tests/scripts/tools/reflow_markdown.py --write ${DOCS[*]}${NC}"
    fail "$(echo "$offenders" | wc -l | tr -d ' ') line(s) a rewrap would shorten"
fi

echo ""
echo -e "${GREEN}Documentation width validation complete!${NC}"
