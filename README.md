# Review packets

Reviewer-agent approvals for CryptoJym/plimsoll pull requests, in the
`plimsoll-review-packet/v1` format that plimsoll-cloud's `github_merge/v2` rule
reads (plimsoll-cloud `docs/ACCEPTANCE-RECEIPTS.md`, "Reviewer-agent approvals
through review packets").

Each file under `packets/` records one review of one exact pull request head.
Its pinned commit, path and SHA-256 are posted on the pull request, before the
merge, as a `plimsoll-review-packet/v1` block. This branch is never merged and
never rewritten: a packet's commit must stay reachable for the receipt to
verify it.
