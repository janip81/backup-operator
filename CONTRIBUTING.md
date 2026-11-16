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

- GitHub Actions builds and pushes images on pushes to `main` and `develop` and on `v*.*.*` tags. Branch pushes that change only ignored paths (docs, manifests/YAML, `.github/**`, …; see `paths-ignore` in the workflow) don't trigger a build.
- PRs are **not** built yet, and the workflow has no manual trigger (tracked in #14). Build locally before opening a PR.

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

If you want to build locally before committing:
```bash
make build
```
Don't push by hand to the tags CI manages (`develop`, `latest`, `v*`); see #14.

---

## 🧾 Release Process

1. Make sure `main` is green  
2. Tag a version:
   ```bash
   git tag -a v0.3.0 -m "Release v0.3.0"
   git push origin v0.3.0
   ```
3. GitHub Actions builds and pushes the `vX.Y.Z` images (`latest` is published from `main` pushes and from version tags)

---

## 🪪 License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
