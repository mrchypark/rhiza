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
6. `sdk/rust/native` is generated from the Go sources and ignored by Git, so it is
   never committed. `sdk/rust/scripts/stage-crate.sh` regenerates it inside a
   throwaway worktree of the release commit, so packaging never reads the
   caller's working tree. `prepare-native.sh` also vendors the Go module closure
   into that tree, which is what makes the published crate self-contained. The
   packaged crate must stay under the crates.io 10 MB limit; `stage-crate.sh`
   prints the size and fails when it exceeds the limit.

## Procedure

1. On a `feature/` branch, set `version = "X.Y.Z"` in `sdk/rust/Cargo.toml`, refresh
   `sdk/rust/Cargo.lock` (CI runs cargo with `--locked`), and point the dependency
   snippets in `README.md` and `sdk/rust/README.md` at it. Open a pull request and
   merge it once CI passes.
2. From the merged `main`, stage the crate outside the repository and publish
   from that stage:

   ```
   stage=$(sdk/rust/scripts/stage-crate.sh origin/main)
   cargo publish --manifest-path "$stage/Cargo.toml" --locked --dry-run
   cargo publish --manifest-path "$stage/Cargo.toml" --locked
   ```

   The stage is built from one commit in a detached worktree, so the package can
   only contain that commit's sources plus the `sdk/rust/native` tree generated
   from them. Cargo sees no repository inside the stage, so there is no
   `--allow-dirty` escape hatch and no way to publish working-tree state.
   Vendoring the Go closure needs the module cache or network access; the
   published crate does not.
3. Confirm the registry serves it, then tag and push:

   ```
   curl -fsS -H 'User-Agent: rhiza' https://crates.io/api/v1/crates/rhizadb/X.Y.Z
   git switch main && git pull
   git tag -a vX.Y.Z -m "Rhiza vX.Y.Z"
   git push origin vX.Y.Z
   ```
4. Create the GitHub release for the tag that now exists.
   `.github/workflows/operator-release.yml` publishes the operator image to GHCR
   from a published release, and `.github/workflows/native-archives.yml` builds
   and attaches the stripped `librhiza_ffi.a` archives for the supported targets
   from the same tag. To attach archives to a release that already exists, run
   that workflow with `gh workflow run native-archives.yml -f tag=vX.Y.Z`.
5. Watch the `Release sync` workflow run for the tag.

## Verification

```
scripts/check-release-sync.sh            # the tag pointing at HEAD
scripts/check-release-sync.sh vX.Y.Z     # an explicit tag
gh run list --workflow=release-sync.yml
gh release view vX.Y.Z --json assets     # attached native archives
```

## Backlog

`v0.15.0` is tagged on a commit with no crates.io release and is not backfilled.
The `v0.15.1` and `v0.15.2` releases that predated 0.15.1 were deleted, because
both were lightweight tags pointing at the `v0.14.1` commit with no crate behind
them; 0.15.1 was then cut from `main` as the first release under these rules.
