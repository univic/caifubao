# Service Token Operations Guide

This guide is for Caifubao administrators to manage service-to-service access using the `manage_service_tokens.py` command-line tool.

## 1. Introduction

Service tokens are used for secure authentication by external services like OpenClaw. They are stored hashed in the database, and only the plain text token is provided once at creation time.

## 2. Command Reference

The management script is located at: `backend/app/scripts/manage_service_tokens.py`.

### 2.1 Creating a Token
`python3 backend/app/scripts/manage_service_tokens.py create`

**Arguments:**
- `--name`: **Required**. A unique name for the service (e.g., `openclaw-prod`)
- `--scopes`: Comma-separated list of scopes (default: `openclaw:data-read`)
- `--expires`: Number of days until expiration (default: 365, set 0 for no expiration)

**Example:**
```bash
python3 backend/app/scripts/manage_service_tokens.py create --name openclaw-dev --expires 90
```

### 2.2 Listing Tokens
`python3 backend/app/scripts/manage_service_tokens.py list`

Displays a summary of all existing tokens, including their status, expiration date, and assigned scopes.

**Example:**
```bash
python3 backend/app/scripts/manage_service_tokens.py list
```

### 2.3 Revoking a Token
`python3 backend/app/scripts/manage_service_tokens.py revoke`

Disables a token immediately.

**Arguments:**
- `--name`: **Required**. The name of the token to revoke.

**Example:**
```bash
python3 backend/app/scripts/manage_service_tokens.py revoke --name openclaw-dev
```

## 3. Best Practices

- **One Token per Service**: Assign unique tokens to each environment or downstream service for better auditing and security.
- **Minimum Scopes**: Only grant the scopes necessary for the task (Principle of Least Privilege).
- **Secure Delivery**: Deliver the plain text token securely and ensure it's not stored in plain text or committed to version control systems by the consumer.
- **Regular Auditing**: Periodically list tokens and revoke those that are no longer in use.
