# Releasing

A release is a tag `vX.Y.Z` on `main`. Pushing the tag runs `.github/workflows/release.yml`,
which re-runs every check, builds `pavi.exe` and `pavi-cli.exe` for Windows x64, and publishes a
GitHub Release with a zip, its SHA-256 checksum, and the CHANGELOG section as notes.

## Steps

1. On a branch, bump the version in the root `Cargo.toml` (`[workspace.package] version`).
   All crates inherit it. Use [Semantic Versioning](https://semver.org/): breaking user-visible
   change → major (minor while 0.x), new feature → minor, fixes only → patch.
2. Run `cargo check` so `Cargo.lock` picks up the new version.
3. In `CHANGELOG.md`, rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`, add a new empty
   `## [Unreleased]` above it, and update the compare links at the bottom.
4. Open a PR, merge it when CI is green.
5. Tag the merge commit and push the tag:

   ```bash
   git switch main && git pull
   git tag -a vX.Y.Z -m "PAVI X.Y.Z"
   git push origin vX.Y.Z
   ```

6. Watch the Release workflow. It fails without publishing if the tag doesn't match the
   workspace version or the CHANGELOG has no section for it.

To redo a failed release: fix on `main`, delete the tag (`git push origin :refs/tags/vX.Y.Z`
and `git tag -d vX.Y.Z`) and the draft release if one was created, then tag again.

## What users get

`pavi-X.Y.Z-windows-x64.zip` with `pavi.exe` (the app; no console window), `pavi-cli.exe`,
`README.md` and `CHANGELOG.md`, plus `pavi-X.Y.Z-windows-x64.zip.sha256`.
Release builds use `[profile.release]` (thin LTO, one codegen unit).

## Code signing (off until configured)

Builds are unsigned, so Windows SmartScreen warns on first launch; the release notes say so.
Signing turns on automatically when both repository secrets exist:

| Secret | Value |
|---|---|
| `WINDOWS_CERT_PFX` | The code-signing certificate (.pfx) as base64: `[Convert]::ToBase64String([IO.File]::ReadAllBytes("cert.pfx"))` |
| `WINDOWS_CERT_PASSWORD` | The .pfx password |

The workflow signs both executables with `signtool` (SHA-256, DigiCert timestamp) before packaging.
An EV or Azure Trusted Signing certificate removes the SmartScreen warning fastest; a standard
OV certificate builds reputation over time.

## Not covered yet

- An app icon and an MSI installer.
- A LICENSE file: without one, published binaries are "all rights reserved".
