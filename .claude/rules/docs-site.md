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
into `REFERENCE.md`.

`[CI: validate_docs.sh (every --flag named exists in --help), validate_doc_links.sh (links and
anchors), validate_doc_width.sh (prose ≤ 100 columns), validate_doc_examples.sh (file-based
examples run and print what the page shows)]` — all discover pages through `git ls-files`. Anchors
follow GitHub's rule exactly: spaces are **not** collapsed, so `a — b` is `#a--b`.

**Row order is not deterministic unless the pipeline asked for it**: DuckDB evaluates in parallel
and `--merge` unions concurrent branches. Give an example an `ORDER BY` when a reader wants stable
output, or compare as a set (`expect_rows`) when ordering would misrepresent the feature. Check an
unseeded `--fake` by shape only. `[unchecked: whether prose is true — write a claim as a runnable
example to make it checked]`
