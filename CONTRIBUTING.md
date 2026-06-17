# Contributing to Kafka Production Patterns

First off, thank you for considering contributing to this reference repository!

## Branching Strategy
We use Trunk-Based Development. All feature branches should branch off `main` and merge into `main` via a Pull Request.

## Conventional Commits
We follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
Format:
```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```
Types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.

## Local Verification
Before opening a Pull Request, run the full verification suite locally:
```bash
make verify
```
This runs the Spotless formatter, Error Prone static analysis, and JaCoCo coverage verification.

## No Em Dashes
As a hard rule for this repository, do not use the em dash character (`—`) anywhere in code, comments, docs, or commits. Use a hyphen (`-`) or restructure the sentence.
