# Queue backends

Orbiter now supports two queue modes.

## In memory queue

The in memory queue is the fastest local option.

Use it when:

1. you are running everything in one process
2. you want low latency local development
3. you do not need queue durability across restarts

This is still the right fit for `:memory:` SQLite runs.

## Store backed queue

The store backed queue uses the `queue_items` table in the selected state
store. That means it works with both SQLite and PostgreSQL.

Use it when:

1. you want queue durability
2. you want a cleaner production shaped runtime boundary
3. you want PostgreSQL coordination semantics through `SKIP LOCKED`
4. you are willing to trade event driven wakeups for a polling loop

## Selection

`orbiter` supports three queue modes:

1. `memory`
2. `store`
3. `auto`

`auto` behaves like this:

1. `:memory:` database uses the in memory queue
2. file backed SQLite or PostgreSQL uses the store backed queue

## Examples

Fast local run:

```bash
orbiter run examples/example_dag.py --db :memory: --queue-backend memory
```

Durable local run:

```bash
orbiter run examples/example_dag.py --db orbiter.db --queue-backend store
```

PostgreSQL runtime:

```bash
orbiter serve examples/example_dag.py \
  --db 'postgresql://orbiter:orbiter@localhost:5432/orbiter' \
  --queue-backend store
```

## Future direction

The next queue step is a dedicated broker backend such as Redis. The current
store backed queue is the bridge between a pure in process prototype and a
full external queue story.
