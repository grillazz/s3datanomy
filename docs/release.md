# Release procedure

We use [trusted publishing with PyPI](https://docs.astral.sh/uv/guides/integration/github/#publishing-to-pypi) for our release.
Check the `.github/workflows/release.yaml` workflow.

Currently in order to release:

- Edit the version on `pyproject.toml`
- Run `uv lock` to update `uv.lock` file
- Create release tag and push `git tag -a v0.2.0 -m v0.2.0`, `git push origin v0.2.0`

Check the github action for the tag was successful, validate the wheel and source distribution is on PyPI
