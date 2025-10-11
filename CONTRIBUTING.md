# 🤝 Contributing to Backup Operator

Thank you for taking the time to improve this project!  
Please follow these simple steps to keep the workflow consistent and clean.

---

## 🧱 Branching Model

We follow a **Git Feature Branch Workflow**:

| Branch | Purpose |
|---------|----------|
| `main` | Production-ready releases (protected) |
| `develop` | Latest tested integration branch |
| `feature/*` | Active work branches for new features or fixes |

### Create a new feature
```bash
git checkout develop
git pull origin develop
git checkout -b feature/<short-description>
```

When finished:
1. Commit and push your changes  
2. Open a Pull Request → base: `develop`  
3. Request review or merge once CI passes  

---

## 🧪 Testing Workflows

- GitHub Actions automatically builds images on push to `develop` or `main`
- To test a workflow manually:
  1. Push to your feature branch  
  2. Go to **Actions → Build and Release Docker Images → Run workflow**

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
or to push manually:
```bash
make push TAG=test
```

---

## 🧾 Release Process

1. Merge `develop` → `main` when stable  
2. Tag a version:
   ```bash
   git tag -a v0.3.0 -m "Release v0.3.0"
   git push origin v0.3.0
   ```
3. GitHub Actions will build and push release images automatically

---

## 🪪 License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
