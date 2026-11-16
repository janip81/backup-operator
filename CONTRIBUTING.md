# 🤝 Contributing to Backup Operator

Thank you for taking the time to improve this project!  
Please follow these simple steps to keep the workflow consistent and clean.

---

## 🧱 Branching Model

We follow a **Git Feature Branch Workflow**:

| Branch | Purpose |
|---------|----------|
| `main` | Default branch; all PRs target it (protected) |
| `develop` | Legacy integration branch; CI also builds it. Don't target it with PRs. |
| `feature/*`, `fix/*`, `docs/*` | Work branches |

### Create a new feature
```bash
git checkout main
git pull origin main
git checkout -b feature/<short-description>
```

When finished:
1. Commit and push your changes  
2. Open a Pull Request → base: `main`, link the issue (`Fixes #N`)  
3. Request review or merge once CI passes  

---

## 🧪 Testing Workflows

- **PRs to `main`:** lint (`ruff`), tests (`pytest`) and both image builds run. Nothing is pushed.
- **Pushes to `main`/`develop`:** the same checks run, then images are pushed with the branch name and `sha-<commit>` tags. Pushes that change only Markdown or `docs/**` are skipped.
- **Version tags `vX.Y.Z`:** images are pushed as `X.Y.Z`, `X.Y` and `latest`.
- `X.Y.Z` and `sha-<commit>` tags are never overwritten: the workflow fails if they already exist. Deploy those (or digests), not branch tags.

---

## 🧰 Commit Conventions

We use [Conventional Commits](https://www.conventionalcommits.org/):

| Type | Purpose |
|------|----------|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `docs:` | Documentation changes |
| `chore:` | Maintenance tasks |
| `refactor:` | Code restructuring |

Example:
```
feat: add automatic restore workflow
fix: handle missing PVC gracefully
```

---

## 🧪 Testing Locally

```bash
make check        # ruff + pytest in a python:3.13 container (same as CI)
make worker-test  # worker image integration tests
make rules-test   # alert rule tests
make build        # local images tagged :local
```
`make push` only accepts throwaway tags (`local*`, `dev-*`, `test-*`). Release tags are built by CI from committed code.

---

## 🧾 Release Process

1. Make sure `main` is green  
2. Tag a version:
   ```bash
   git tag -a v0.3.0 -m "Release v0.3.0"
   git push origin v0.3.0
   ```
3. GitHub Actions builds and pushes `X.Y.Z`, `X.Y` and `latest`

---

## 🪪 License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
