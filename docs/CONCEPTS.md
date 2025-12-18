# Core Concepts & Architecture of cyberbrain (current state)

Cyberbrain is an event‑driven graph orchestration framework. It maps data into a typed graph (knowledge graph), matches declarative action dependencies as patterns, and schedules idempotent jobs executed by worker neurons to iteratively enrich the graph.

This document reflects the current, implemented system — not a roadmap.

---

## Glossary (project terms → industry terms)

- Memory (Gits) → in‑memory graph store / knowledge graph
- Cortex → action registry / dependency catalog
- Scheduler → event processor / pattern matcher / orchestrator
- Neuron → worker / runner
- Job → task / work item
- Demultiplexer (slot‑based) → combination builder over dependency slots
- Memory/Witness → local idempotency token (anchor‑scoped) — no global index

---

## Core components (how they work now)

### Memory: Gits (graph store)
- Stores entities and relations (typed, versioned). Identity is the pair (Type, ID). IDs are type‑scoped (not globally unique).
- Mapper writes here; Scheduler and Neurons read/write Jobs and light metadata.

### Cortex: action registry
- Registers actions and maps each action’s GetConfig() dependency tree into the graph.
- Exposes dependencies to the Scheduler. Dependencies may include aliases to distinguish same‑type siblings.

### Scheduler: overlay‑only, alias‑aware matcher
- Trigger input: a batch of mapped deltas (entities with bMap and relation‑only edges). The scheduler extracts precise anchors from this batch.
- Pattern compilation & cache: dependencies are compiled once into PatternNode trees (Alias, Type, Mode, Filters) with deterministic child order and normalized filter fields (e.g., Properties.Transport → Transport). Cache is bounded by Action|Dep key.
- Overlay matching: for each anchor, overlay the dependency pattern onto the stored graph and build an alias→(Type,ID) assignment via bounded expansion along the declared relations and filters. Backtracks if a slot cannot be satisfied; explores all anchors in the batch.
- Strict causality: a candidate input is eligible only if it contains at least one updated entity from this batch or (for relation‑only deltas) the specific updated child endpoint. Shared parents alone do not trigger siblings.
- Slot demux (when needed): demultiplex across alias slots only (Cartesian across slots that admit multiple candidates), with deep‑copied inputs so combinations are immutable.
- Idempotency (no global index): before creating a Job, check/create a local Memory/Witness under a deterministic anchor.

### Neuron: worker that executes jobs
- Repeatedly claims an open Job, calls the action’s Execute, and maps the returned result back into Memory. This decentralizes scheduling: results feed back to mapping → scheduling.
- Injects optional dependencies (Gits/Mapper/Logger) if the action implements the respective setters.

### Demultiplexer: slot‑level utility
- Used inside the scheduler to fan out across dependency slots (aliases), not as a global, pre‑matching phase. It deep‑copies entities to keep combinations independent.

### Job: unit of work
- Captures the action name, dependency name, and the constructed input graph slice. Persisted in Memory and consumed by Neurons.

### Observer: system watchdog (optional)
- Monitors “no open jobs and neurons idle,” useful in tests and demos. Not required for scheduling.

### Archivist: logging
- Structured logs, with “scheduling …” prefixes added in the scheduler/neuron to support grep‑friendly tracing.

---

## Mapper identity & delta semantics (critical for correctness)

- Special ID values when emitting entities from actions:
  - `ID = -1` → force create (Type, Value, Context as given).
  - `ID = -2` → match by (Type & Value) globally; create if none exists.
  - `ID = 0` → match by (parents + Type & Value); create if no such related entity exists under that parent.
- Addressing existing nodes: if you include entities addressed by (Type, ID) in your returned structure, the mapper will map onto those existing entities. You can also create a new relation between two existing nodes by nesting both addressed entities.
- Delta mark (`bMap`): Mapper sets `bMap` on created/updated entities. For relation‑only deltas, the scheduler considers the child endpoint as the updated element for causality.
- Context: not used for scheduling or signatures. Use it as free‑form execution metadata; keep identity in (Type, ID) (or match via Value with `ID:-2`).

---

## Idempotency without a global index (Memory/Witness)

![Witness flow](../diagrams/witness_flow.svg)

- Deterministic anchor: choose an anchor entity from the candidate input (e.g., the Primary or a stable tie‑break).
- Memory entity: create/read a local “Memory” (aka Witness) node with Context `Exec:<Action>:<Dep>` and Value = signatureHash(Action|Dep|anchor|ordered‑alias IDs).
- Link anchor → Memory. If it already exists, skip creating a new Job (duplicate). If it’s new, persist the Job. No global tables; only anchor‑local adjacency is used.

---

## End‑to‑end flow (current)

![End‑to‑end overview](../diagrams/overview.svg)

A concise, step‑by‑step description of the runtime loop:

1. Learn: External data (or action results) are submitted. The Mapper integrates the payload into the graph and marks created/updated entities with bMap; relation‑only changes are represented as edges.
2. Anchor discovery: The Scheduler reads the batch and extracts precise anchors: entities with bMap and, for relation‑only deltas, the updated child endpoints.
3. Pattern overlay: For each candidate action/dependency, the Scheduler overlays the compiled dependency pattern on the stored graph starting at the anchor, expanding only along declared edges and filters (alias‑aware).
4. Strict causality: A constructed candidate input is only eligible if it contains at least one updated entity from this batch or (for relation‑only deltas) the specific updated child endpoint. Shared parents alone are insufficient.
5. Idempotency check: The Scheduler computes a signature (Action|Dep|anchor|ordered aliases) and performs a local anchor‑scoped Memory/Witness check. If a matching Memory exists, the candidate is skipped; otherwise a new Memory is created and a Job is persisted.
6. Execution: A Neuron claims the Job, executes the action, and returns results (transport entities).
7. Feedback: The Mapper integrates the results (setting bMap on any new/changed entities), which produces the next batch for the Scheduler.

---

## Scheduling internals at a glance

![Scheduling flow](../diagrams/scheduling_flow.svg)

The scheduler’s internal steps for each batch, without diagrams:

1. Inputs: Receive a batch containing entities with bMap and/or relation‑only edges created in this cycle.
2. Pattern prep: Fetch/compile the dependency pattern (cached per Action|Dep) with deterministic child order and normalized filter fields.
3. Expansion: For each anchor, expand along the pattern to build an alias→(Type,ID) assignment. Backtrack if a slot cannot be satisfied; explore all anchors in the batch.
4. Relevance & causality:
   - Apply filter relevance for Match nodes (updated keys must intersect the filter fields).
   - Enforce strict causality: the updated entity (or relation‑child) must be present in the constructed input.
5. Witness CAS: On each eligible candidate, compute the signature and attempt an anchor‑local Memory/Witness create; if it already exists, skip, otherwise proceed.
6. Persist Job: Store the Job (action, dependency, input graph slice). Neurons will pick it up.
7. Results: After execution, the returned results are mapped (bMap set as appropriate), which in turn can seed the next scheduler pass.

---

## Practical guardrails (for action authors)

- Use `ID:-2` for anchors like IP/Domain to merge by (Type & Value) globally; reserve `ID:0` for parent‑scoped matches; use `ID:-1` only to force creation.
- Keep identity stable (Value normalization); don’t embed per‑run unique tokens into identity fields.
- Prefer anchoring on existing entities by (Type, ID) when enriching; create relations between existing nodes by nesting both addressed entities.
- Don’t rely on Context for matching or signatures; scheduling is context‑agnostic unless explicitly modeled.

---

## Key properties (why this scales)

- Bounded matching: overlay expands only along declared dependency edges and filters; alias slots constrain recombination.
- Local idempotency: witness is anchor‑local, no global dedupe index.
- Deterministic: compiled pattern order and alias‑based signatures keep inputs stable.
- Observability: scheduler logs are prefixed with “scheduling …”; use analyzeLog.php to detect repeats (creates, persists, skips, histograms).

---

## Quick reference

- Identity = (Type, ID); IDs are type‑scoped. Match by value with `ID:-2`.
- Causality = updated entity or relation‑child must be in the constructed input.
- Demux = across alias slots only; inputs are deep‑copied (immutable).
- No global indices for dedupe; use anchor→Memory(signature) locally.
