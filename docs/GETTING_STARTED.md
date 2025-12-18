# Getting Started with cyberbrain

This guide shows the fastest way to spin up a minimal cyberbrain, register an action, feed initial data, and let the scheduler create jobs. It reflects the current overlay‑only scheduler, strict causality, and local Memory/Witness idempotency.

---

> Visual overview of the runtime loop:
>
> ![Overview](../diagrams/overview.svg)

---

## 1) Create a new project

Initialize a Go module and add cyberbrain:

```
mkdir my-cyberbrain && cd my-cyberbrain
go mod init example.com/my-cyberbrain
go get github.com/voodooEntity/go-cyberbrain@latest
```

Recommended tree:

```
my-cyberbrain/
├── cmd/app/main.go        # entrypoint
├── src/                   # your custom actions (optional)
├── go.mod
└── go.sum
```

---

## 2) Minimal program

Create `cmd/app/main.go` and wire your logger, brain, actions, and start it. The high‑level steps are:

```
1) Create a standard Go logger (flags 0 recommended for clean Cyberbrain output).
2) New cyberbrain with Settings { Ident, NeuronAmount>=1, LogLevel, Logger }.
3) Register one or more Actions (see docs/ACTIONS.md for building configs).
4) Start() to launch neurons.
5) LearnAndSchedule(...) to feed initial data.
```

Notes:
- Always run with at least one neuron. `NeuronAmount: 0` defaults to the number of logical CPUs.
- Identity in storage is `(Type, ID)`; IDs are per‑type.

---

## 3) Registering real Actions

Actions define their dependency graphs and the logic to execute. Register them before `Start()`:

```
cb.RegisterAction("myActionName", myActionFactory)
```

Your action’s `GetConfig()` returns a config built with the fluent `configBuilder` (see docs/ACTIONS.md). The scheduler compiles the dependency once (alias‑aware) and matches it on each batch.

Links:
- Developing Actions: [docs/ACTIONS.md](ACTIONS.md)
- Core Concepts: [docs/CONCEPTS.md](CONCEPTS.md)

---

## 4) Feeding data and scheduling

Use `LearnAndSchedule` to ingest deltas. The mapper flags new/updated items with `bMap`; the scheduler constructs inputs that include those updates and creates Jobs.

ID semantics when building results/inputs:
- `ID = -1`: force create new entity.
- `ID = -2`: match by (Type, Value) or create if absent (use for anchors like IP/Domain to avoid duplicates).
- `ID = 0`: try parent+Value match else create (scoped to the related parent).

Example seed:

```
cb.LearnAndSchedule(transport.TransportEntity{ ID:-1, Type:"Domain", Value:"example.com", Context:"Data" })
```

---

## 5) Observing and graceful stop (optional)

For batch runs you can wait until all jobs complete using the Observer:

```
obsi := cb.GetObserverInstance(func(_ interface{}) {}, true)  # lethal=true stops the system
obsi.Loop()  # blocks until no open jobs and neurons are idle
```

In services, prefer your own lifecycle/signals and let neurons run continuously.

---

## 6) Tips and guardrails

- Keep Context consistent, but it’s not used for scheduling or signatures.
- For redirects or joins that reference existing anchors (e.g., IP for a new Domain), use `ID:-2` to merge by value instead of creating duplicates.
- The scheduler is idempotent via local Memory/Witness per anchor; repeated identical inputs are skipped.

---

## 7) Next steps

- Read: [Core Concepts & Architecture](CONCEPTS.md)
- Build: [Developing Custom Actions](ACTIONS.md)
- Explore: run the included sample:

```
go run ./cmd/example
```
