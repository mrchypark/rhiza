# Releasing Rhiza

One version number identifies a release, and it appears in three places at once.
`vX.Y.Z` is a release only while all three agree:

| Place | Value | Role |
| --- | --- | --- |
| `sdk/rust/Cargo.toml` | `version = "X.Y.Z"` | source of truth |
| Git tag | `vX.Y.Z`, annotated | derived |
| crates.io | `rhizadb X.Y.Z`, not yanked | derived |

`scripts/check-release-sync.sh` verifies all three.
`.github/workflows/release-sync.yml` runs it on every `v*` tag, so a tag that
is not backed by a published crate fails CI.

## Rules

1. `sdk/rust/Cargo.toml` is the only place a release number is set. The tag and
   the crate version always equal it. Never advance one without the others.
2. Publish before tagging. A tag with no crates.io release is not a release and
   is not announced as one.
3. Tags are annotated: `git tag -a vX.Y.Z -m "Rhiza vX.Y.Z"`. The GitHub release
   UI creates lightweight tags, so create the tag locally and push it; a
   lightweight tag fails `check-release-sync.sh`.
4. Never move or reuse a published tag or version. crates.io versions are
   immutable, so a moved tag misrepresents the artifact it points at.
5. Tag the merge commit on `main` that CI passed, never a branch tip.
6. `prepare-native.sh` regenerates `sdk/rust/native` from the Go sources. That tree
   is generated and ignored by Git, so it is never committed; run the script
   immediately before packaging, as the release step below does.

## Procedure

1. On a `feature/` branch, set `version = "X.Y.Z"` in `sdk/rust/Cargo.toml`, refresh
   `sdk/rust/Cargo.lock` (CI runs cargo with `--locked`), and point the dependency
   snippets in `README.md` and `sdk/rust/README.md` at it. Open a pull request and
   merge it once CI passes.
2. From the merged `main`, regenerate the bundled Go tree and publish:

   ```
   sh sdk/rust/scripts/prepare-native.sh
   cargo publish --manifest-path sdk/rust/Cargo.toml --locked --allow-dirty --dry-run
   cargo publish --manifest-path sdk/rust/Cargo.toml --locked --allow-dirty
   ```

   `--allow-dirty` is required: the package includes `sdk/rust/native`, which is
   generated and ignored by Git.
3. Confirm the registry serves it, then tag and push:

   ```
   curl -fsS -H 'User-Agent: rhiza' https://crates.io/api/v1/crates/rhizadb/X.Y.Z
   git switch main && git pull
   git tag -a vX.Y.Z -m "Rhiza vX.Y.Z"
   git push origin vX.Y.Z
   ```
4. Create the GitHub release for the tag that now exists.
   `.github/workflows/operator-release.yml` publishes the operator image to GHCR
   from a published release.
5. Watch the `Release sync` workflow run for the tag.

## Verification

```
scripts/check-release-sync.sh            # the tag pointing at HEAD
scripts/check-release-sync.sh vX.Y.Z     # an explicit tag
gh run list --workflow=release-sync.yml
```

## Backlog

`rhizadb` published nothing between 0.14.1 and 0.15.1. `v0.15.1` and `v0.15.2`
were deleted on 2026-09-19; both were lightweight tags pointing at the `v0.14.1`
commit with no crate behind them. `v0.15.0` stays as a tag on a distinct commit
and is not backfilled.
