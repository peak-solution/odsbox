# Repository Rules
## Branching strategy

### Main Branches

- **`main`**: This branch contains the stable, production-ready code.
  Only thoroughly tested and approved changes are merged here.
- **`dev`**: This is the integration branch for features and fixes.
  All new development work is merged here before it goes to `main`.

### Supporting Branches

- **Feature Branches**: Used for developing new features.
  These branches are created from `dev` and merged back into `dev` once the feature is complete.
    - Naming convention: `feature/feature-name`
- **Bugfix Branches**: Used for fixing bugs. These branches are also created
  from `dev` and merged back into `dev` after the fix.
    - Naming convention: `bugfix/bug-name`
- **Release Branches**: When preparing for a new release, a release branch is created from `dev`.
  This branch is used for final testing and minor bug fixes before merging into `main`.
    - Naming convention: `release/x.y.z`
- **Hotfix Branches**: For critical fixes that need to be applied to the production code immediately.
  These branches are created from `main` and merged back into both `main` and `dev`.
    - Naming convention: `hotfix/x.y.z`

### Workflow Example

#### Feature Development

- Create a feature branch from `dev`:<br/>`git checkout -b feature/new-feature dev`
- Develop the feature and commit changes.
- Merge the feature branch back into `dev`:<br/>`git checkout dev && git merge feature/new-feature`
- Delete the feature branch:<br/>`git branch -d feature/new-feature`

#### Release Preparation

- Create a release branch from `dev`:<br/>`git checkout -b release/1.0.0 dev`
- Perform final testing and bug fixes.
- Merge the release branch into `main`:<br/>`git checkout main && git merge release/1.0.0`
- Tag the release:<br/>`git tag -a v1.0.0 -m "Release 1.0.0"`
- Merge the release branch back into `dev`:<br/>`git checkout dev && git merge release/1.0.0`
- Delete the release branch:<br/>`git branch -d release/1.0.0`

#### Hotfix

- Create a hotfix branch from `main`:<br/>`git checkout -b hotfix/1.0.1 main`
- Apply the hotfix and commit changes.
- Merge the hotfix branch into `main`:<br/>`git checkout main && git merge hotfix/1.0.1`
- Tag the hotfix:<br/>`git tag -a v1.0.1 -m "Hotfix 1.0.1"`
- Merge the hotfix branch into `dev`:<br/>`git checkout dev && git merge hotfix/1.0.1`
- Delete the hotfix branch:<br/>`git branch -d hotfix/1.0.1`

## Merge Requests

Merge request must follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) naming rules.
