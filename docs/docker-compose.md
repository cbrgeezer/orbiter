# Docker Compose deployment

Orbiter now ships with a minimal Docker Compose bundle for a production shaped
local deployment story.

## What it starts

1. PostgreSQL 16
2. Orbiter API service
3. Orbiter scheduler service
4. Orbiter worker service
5. a Caddy reverse proxy in front of the API
6. the browser operator console served from the API process

## Start the stack

```bash
docker compose up --build
```

Once the stack is healthy, open:

```text
http://localhost:8000/ui/
```

The default compose bundle enables API key auth on the API service with the
value:

```text
orbiter-demo-key
```

Override it before startup:

```bash
export ORBITER_API_KEY='replace-this-in-real-use'
docker compose up --build
```

## Runtime configuration

The default compose bundle runs:

1. PostgreSQL as the state store
2. the durable store backed queue
3. `examples/example_dag.py` as the shared DAG definition
4. split runtime roles instead of a single all in one process

The container image uses a role based entrypoint. `docker-compose.yml` selects
the runtime by setting `ORBITER_ROLE` to `api`, `scheduler`, or `worker`.
The same entrypoint can also run `all` for a single process deployment.

When `ORBITER_DB` points at PostgreSQL, the entrypoint waits for the database
before starting the selected role.

## What this bundle is for

It is designed for:

1. local demos
2. product walkthroughs
3. architecture validation
4. showing a serious deployment story on the repository

It is not yet a full production packaging story. There is still no:

1. automatic TLS termination in the bundled proxy
2. secrets management
3. autoscaling story for workers
4. external object storage or audit retention story

Those are next layer concerns, not omissions hidden behind marketing.
