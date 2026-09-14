#!/usr/bin/env python3
"""Resolve every relative Markdown link in the docs/ site.

A page that links to a file that moved, or to a heading that was reworded, is broken in exactly
the way a reader notices and a writer does not: the link renders, it just goes nowhere. Twenty-two
pages cross-link each other about a hundred times, so the failure is a matter of when.

Anchors follow GitHub's rule and not an approximation of it: lower-case, drop everything that is
not a word character, a space or a hyphen, then turn each remaining space into a hyphen. Runs of
spaces are NOT collapsed -- "a -- b" yields "a--b" -- which is the detail an intuitive
implementation gets wrong, reporting three correct links as broken.

Exit 0 when every link resolves, 1 otherwise, printing file:line and what was not found.
"""

import os
import re
import sys

LINK = re.compile(r'\[[^\]]*\]\(([^)\s]+)\)')
HEADING = re.compile(r'^(#{1,6})\s+(.*?)\s*$')


def slug(title: str) -> str:
    t = title.strip().lower().replace('`', '')
    t = re.sub(r'[^\w\s-]', '', t, flags=re.UNICODE)
    return t.replace(' ', '-')


def anchors_of(path: str) -> set[str]:
    found = set()
    with open(path, encoding='utf-8') as fh:
        fenced = False
        for line in fh:
            if line.lstrip().startswith('```'):
                fenced = not fenced
                continue
            if fenced:
                continue
            m = HEADING.match(line)
            if m:
                found.add(slug(m.group(2)))
    return found


def main(pages: list[str]) -> int:
    cache: dict[str, set[str]] = {}
    problems = []

    for page in pages:
        with open(page, encoding='utf-8') as fh:
            for n, line in enumerate(fh, 1):
                for target in LINK.findall(line):
                    if target.startswith(('http://', 'https://', 'mailto:', '#!')):
                        continue
                    path, _, frag = target.partition('#')
                    resolved = os.path.normpath(
                        os.path.join(os.path.dirname(page), path)) if path else page
                    if not os.path.exists(resolved):
                        problems.append(f'{page}:{n}: no such file -- {target}')
                        continue
                    if not frag:
                        continue
                    if resolved not in cache:
                        cache[resolved] = anchors_of(resolved)
                    if frag not in cache[resolved]:
                        problems.append(f'{page}:{n}: no such heading -- {target}')

    for p in problems:
        print(p)
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
