# Security Policy

## Supported Scope

Caifubao is currently an MVP project. Security fixes are prioritized for the
current main development line.

## Reporting a Vulnerability

Please do not open public issues for suspected vulnerabilities. Report security
issues privately to the project maintainers with:

- A short description of the issue.
- Steps to reproduce, if available.
- Affected modules or files.
- Any known impact.

## Secret Handling

Do not commit real credentials, tokens, kubeconfigs, database dumps, or local
environment files. Use `.env.example` files for placeholders only.

If a secret is ever committed to Git history, treat it as exposed:

1. Rotate the secret immediately.
2. Remove it from current code and configuration.
3. Publish only a cleaned history or a fresh public repository.

## Financial Disclaimer

This project is for research, learning, and demonstration purposes only. It does
not provide investment advice, trading recommendations, or financial services.
