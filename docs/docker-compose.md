# Docker Compose deployment

Orbiter now ships with a minimal Docker Compose bundle for a production shaped
local deployment story.

## What it starts

1. PostgreSQL 16
2. Orbiter API service
3. Orbiter scheduler service
4. Orbiter worker service
5. the browser operator console served from the API process

## Start the stack

```bash
docker compose up --build
```

Once the stack is healthy, open:

```text
http://localhost:8000/ui/
```

## Runtime configuration

The default compose bundle runs:

1. PostgreSQL as the state store
2. the durable store backed queue
3. `examples/example_dag.py` as the shared DAG definition
4. split runtime roles instead of a single all in one process

The current command is intentionally explicit in `docker-compose.yml` so the
runtime story stays legible.

## What this bundle is for

It is designed for:

1. local demos
2. product walkthroughs
3. architecture validation
4. showing a serious deployment story on the repository

It is not yet a full production packaging story. There is still no:

1. reverse proxy
2. authentication layer
3. secrets management
4. autoscaling story for workers

Those are next layer concerns, not omissions hidden behind marketing.
