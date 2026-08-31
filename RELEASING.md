# Releasing

`pyproject.toml` is the only place the version lives. Releases are tagged on
`main` — there are no `release/*` branches.

**Publishing to PyPI is irreversible.** A version can never be re-uploaded, even
after deleting it. If something is wrong, go forward to the next version.

## Release

`.github/workflows/release.yml` — **Run workflow** on the Actions tab, from
`main`. It builds, publishes to PyPI, pushes the tag, creates the GitHub
release, and bumps the version on `main`.

| Input | Default | Meaning |
| --- | --- | --- |
| `next-version` | `prerelease` | Version set on `main` after the release. A poetry bump rule, or an explicit version like `3.1.0b1`. |
| `whats-new` | empty | Text placed above the generated changelog. |

Release notes are generated from merged PR titles, so write PR titles as
changelog entries.

The bump leaves `main` one version ahead of the newest tag.

## Release by hand

If the workflow is unavailable:

```bash
poetry version -s                  # what is about to ship
poetry build
poetry publish                     # needs: poetry config pypi-token.pypi <token>

git tag -a "v$(poetry version -s)" -m "Release v$(poetry version -s)"
git push origin "v$(poetry version -s)"
gh release create "v$(poetry version -s)" --generate-notes

poetry version prerelease
git commit -am "Bump version to $(poetry version -s)"
git push origin main
```

## Build

`.github/workflows/build.yml` runs on every push to `main`: builds the wheel and
sdist, checks them, uploads them to the run. It never publishes, and it does not
run tests — they need Earth Engine credentials.
