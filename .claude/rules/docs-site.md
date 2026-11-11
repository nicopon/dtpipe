---
paths:
  - "docs/**"
  - "README.md"
  - "REFERENCE.md"
  - "EXTENDING.md"
---

# The docs/ site

A task-oriented layer **over** the root documents. **The site explains and shows; `REFERENCE.md`
enumerates**: flag tables go there, walkthroughs in `docs/guides/`. End every page with a pointer
into `REFERENCE.md`. `REFERENCE.md` lists the flags in common use and says so; `dtpipe --help`,
generated from the binary, is the complete list, and no page calls `REFERENCE.md` exhaustive.

`[CI: validate_docs.sh (every --flag named exists in --help), validate_doc_links.sh (links and
anchors, of the site and the root documents), validate_doc_width.sh (prose ≤ 100 columns),
validate_doc_examples.sh (file-based examples run and print what the page shows)]` — all discover
pages through `git ls-files`. Anchors follow GitHub's rule exactly: spaces are **not** collapsed,
so `a — b` is `#a--b`.

## How a page is written

A person reads a page to do something. **Show the example first, then explain it in one to three
sentences**; a one-line lead-in may come before it.

- **One fact, one page.** Another page that needs it links there and never restates it: a fact
  told twice drifts apart.
- **Say what it does, not what it is not.** No defence of a design choice and no comparison with
  other tools. Why a choice was made belongs in a `CLAUDE.md` or a rule, not on the site.
- **Nothing a user cannot observe.** No type or method name, no hash composition, no internal
  conversion path. A user sees flags, files, output, messages and exit codes. A page whose reader
  writes .NET code (`EXTENDING.md`, `docs/dotnet.md`) names the types that reader calls.
- **Pitfalls go last**, as a list, one line each.
- **The experimental family lives in `docs/experimentation.md` and its walkthrough.** Any other
  page gives it one link at most.
- **A guide stays under about 800 words, a sentence under about 25.** A walkthrough that runs a
  whole setup from nothing may be longer.

`[unchecked]`

**Row order is not deterministic unless the pipeline asked for it**: DuckDB evaluates in parallel
and `--merge` unions concurrent branches. Give an example an `ORDER BY` when a reader wants stable
output, or compare as a set (`expect_rows`) when ordering would misrepresent the feature. Check an
unseeded `--fake` by shape only. `[unchecked: whether prose is true — write a claim as a runnable
example to make it checked]`
