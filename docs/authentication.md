# Authentication

Orbiter now supports an API key layer for the operator surface.

## Model

The current model is intentionally small:

1. one shared API key
2. header based authentication
3. browser console support through a local key prompt

This is not a substitute for full identity management, but it is a real
protection layer for self hosted deployments.

## Configuration

Set `ORBITER_API_KEY` in the API process environment.

Example:

```bash
export ORBITER_API_KEY='replace-this-with-a-strong-key'
orbiter serve-api examples/example_dag.py --db orbiter.db
```

## Accepted headers

Orbiter accepts either:

1. `X-Orbiter-Key: <key>`
2. `Authorization: Bearer <key>`

## Public routes

These routes remain public so the browser console can load and discover
whether authentication is enabled:

1. `/`
2. `/healthz`
3. `/auth/config`
4. `/ui/*`

All operational API routes require a valid key when authentication is
enabled.

## Console behaviour

The browser console prompts for the API key and stores it in browser local
storage so it can attach the key to subsequent API requests.

This is acceptable for a lightweight self hosted operator console, but it is
not a replacement for a full server side session model. If the deployment
needs stricter controls, put Orbiter behind a reverse proxy with a stronger
authentication layer.
