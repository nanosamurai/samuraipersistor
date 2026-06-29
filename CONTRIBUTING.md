# Contributing

Thanks for your interest in contributing!

## Development workflow
- Create a feature branch from `master`.
- Keep commits small and focused.
- Add/update tests when changing behavior.

## Running checks locally
See `.github/workflows/ci.yml` for the authoritative commands.

## Security
- Never commit secrets (tokens, API keys, private keys).
- Prefer `.env.example` over `.env`.
- CI runs secret scanning (gitleaks). Treat failures as blockers.