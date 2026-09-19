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
6. `prepare-native.sh` rewrites `sdk/rust/native` from the Go sources. Run it and
   commit the result in the release pull request, so the tag, the checkout, and
   the packaged crate carry the same Go code.

## Procedure

1. On a `feature/` branch, bump the version and refresh the bundle:

   ```
   sh sdk/rust/scripts/prepare-native.sh
   # set version = "X.Y.Z" in sdk/rust/Cargo.toml
   ```

   Commit both, open a pull request, and merge it once CI passes.
2. Publish the crate from the merged `main`:

   ```
   cargo publish --manifest-path sdk/rust/Cargo.toml --locked --dry-run
   cargo publish --manifest-path sdk/rust/Cargo.toml --locked
   ```
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

Tags `v0.15.0` through `v0.15.2` predate this rule and have no crates.io
release; `rhizadb` 0.14.1 is the newest published version. They are not
backfilled. The next release from `main` resynchronises the three channels.
