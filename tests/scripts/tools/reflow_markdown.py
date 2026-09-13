#!/usr/bin/env python3
"""Keep Markdown prose within a column width, leaving code, tables and headings untouched.

Two modes over the same parser:

  (no flag)  list the lines a rewrap would shorten, exit 1 if any
  --write    rewrap them in place

Only line breaks move. `--write` proves it before touching the file: both the source and the
result are reduced to a canonical form — every block joined onto one line — and the write is
refused unless the two match.
"""
import argparse
import difflib
import re
import sys

FENCE = re.compile(r'^ {0,3}(`{3,}|~{3,})')
HEADING = re.compile(r'^ {0,3}#{1,6}(\s|$)')
TABLE = re.compile(r'^\s*\|')
HR = re.compile(r'^ {0,3}([-*_])(\s*\1){2,}\s*$')
BULLET = re.compile(r'^(\s*)([-*+])(\s+)(.*)$')
ORDERED = re.compile(r'^(\s*)(\d{1,9}[.)])(\s+)(.*)$')
QUOTE = re.compile(r'^(\s*>\s?)(.*)$')
HTML = re.compile(r'^\s*<')
# A GitHub alert marker owns its line: joined to the text below, the callout stops rendering.
ALERT = re.compile(r'^\s*\[!\w+\]\s*$')
# What a continuation line must never start with, or the paragraph grows a block of its own.
DANGEROUS = re.compile(r'^(([-*+>|]|#{1,6}|\d{1,9}[.)])(\s|$)|`{3,}|~{3,}|={2,}\s*$|-{2,}\s*$)')


def link_open(token):
    return token.count('[') > token.count(']') or bool(re.search(r'\]\([^)]*$', token))


def atoms(text):
    """Split on whitespace, then re-join what a line break would damage or obscure."""
    toks = text.split()
    out = []
    i = 0
    while i < len(toks):
        cur = toks[i]
        i += 1
        while i < len(toks) and (link_open(cur) or (cur.endswith(']') and toks[i].startswith('('))):
            cur += ' ' + toks[i]
            i += 1
        while cur.count('`') % 2 == 1 and i < len(toks):
            cur += ' ' + toks[i]
            i += 1
        out.append(cur)
    return out


def wrap(text, width, first_prefix, cont_prefix):
    words = atoms(text)
    if not words:
        return [first_prefix.rstrip()]
    lines = []
    prefix = first_prefix
    cur = ''
    for word in words:
        candidate = word if not cur else cur + ' ' + word
        if cur and len(prefix) + len(candidate) > width:
            lines.append((prefix, cur))
            prefix = cont_prefix
            cur = word
        else:
            cur = candidate
    lines.append((prefix, cur))
    fixed = []
    for index, (pfx, body) in enumerate(lines):
        if index > 0 and DANGEROUS.match(body):
            head, _, rest = body.partition(' ')
            prev_prefix, prev_body = fixed[-1]
            fixed[-1] = (prev_prefix, prev_body + ' ' + head)
            if not rest:
                continue
            body = rest
        fixed.append((pfx, body))
    return [(pfx + body).rstrip() for pfx, body in fixed]


def reflow(lines, width, trigger):
    """Rewrap every block holding a line longer than `trigger`; copy the rest verbatim."""
    out = []
    i = 0
    total = len(lines)
    while i < total:
        line = lines[i]
        opening = FENCE.match(line)
        if opening:
            marker = opening.group(1)
            out.append(line)
            i += 1
            while i < total:
                out.append(lines[i])
                closing = FENCE.match(lines[i])
                i += 1
                if closing and closing.group(1)[0] == marker[0] and len(closing.group(1)) >= len(marker):
                    break
            continue
        if (not line.strip() or HEADING.match(line) or TABLE.match(line) or HR.match(line)
                or HTML.match(line) or ALERT.match(line)):
            out.append(line)
            i += 1
            continue
        if QUOTE.match(line):
            start = i
            while i < total and QUOTE.match(lines[i]):
                i += 1
            block = lines[start:i]
            prefix = QUOTE.match(block[0]).group(1)
            prefix = prefix if prefix.endswith(' ') else prefix + ' '
            if max(len(b) for b in block) > trigger:
                inner = [QUOTE.match(b).group(2) for b in block]
                out.extend((prefix + b).rstrip()
                           for b in reflow(inner, width - len(prefix), trigger - len(prefix)))
            else:
                out.extend(block)
            continue
        item = BULLET.match(line) or ORDERED.match(line)
        if item:
            indent, marker, gap, rest = item.groups()
            first_prefix = indent + marker + gap
            start = i
            i += 1
            parts = [rest]
            while i < total and not breaks_block(lines[i]) and not (BULLET.match(lines[i]) or ORDERED.match(lines[i])):
                parts.append(lines[i].strip())
                i += 1
            block = lines[start:i]
            if max(len(b) for b in block) > trigger:
                out.extend(wrap(' '.join(parts), width, first_prefix, ' ' * len(first_prefix)))
            else:
                out.extend(block)
            continue
        start = i
        parts = []
        while i < total and not breaks_block(lines[i]):
            if i > start and (BULLET.match(lines[i]) or ORDERED.match(lines[i])):
                break
            parts.append(lines[i].strip())
            i += 1
        block = lines[start:i]
        indent = re.match(r'^(\s*)', block[0]).group(1)
        if max(len(b) for b in block) > trigger:
            out.extend(wrap(' '.join(parts), width, indent, indent))
        else:
            out.extend(block)
    return out


def breaks_block(line):
    return bool(not line.strip() or FENCE.match(line) or TABLE.match(line) or HEADING.match(line)
                or HR.match(line) or QUOTE.match(line) or HTML.match(line) or ALERT.match(line))


def split(path):
    text = open(path, encoding='utf-8').read()
    lines = text.split('\n')
    trailing = lines and lines[-1] == ''
    return (lines[:-1] if trailing else lines), trailing


def canonical(lines):
    return reflow(lines, 10 ** 7, -1)


def offenders(lines, width):
    """Lines over `width` that the rewrap would actually shorten.

    Asking the formatter is what keeps the check honest: a line whose overflow is one
    unbreakable atom — a URL, a link, an inline code span — comes back unchanged and is
    not reported, because no line break makes it shorter.
    """
    result = reflow(lines, width, width)
    changed = set()
    for tag, i1, i2, _, _ in difflib.SequenceMatcher(None, lines, result, autojunk=False).get_opcodes():
        if tag != 'equal':
            changed.update(range(i1, i2))
    return [(n + 1, len(lines[n]), lines[n]) for n in sorted(changed) if len(lines[n]) > width]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('files', nargs='+')
    parser.add_argument('--width', type=int, default=100)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()

    failed = False
    for path in args.files:
        lines, trailing = split(path)
        if args.write:
            result = reflow(lines, args.width, args.width)
            if canonical(lines) != canonical(result):
                print(f'{path}: refused — the rewrap changed more than line breaks', file=sys.stderr)
                failed = True
                continue
            if result != lines:
                open(path, 'w', encoding='utf-8').write('\n'.join(result) + ('\n' if trailing else ''))
                print(f'{path}: rewrapped')
            continue
        for number, length, line in offenders(lines, args.width):
            print(f'{path}:{number}: {length} chars — {line[:60]}…')
            failed = True
    return 1 if failed else 0


sys.exit(main())
