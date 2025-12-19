<h1 align="center">
  <br>
  <a href="https://github.com/voodooEntity/cyberbrain/"><img src="logo.png" alt="cyberbrain" width="235"></a>
  <br>
</h1>

> Disclaimer: This is an experimental research project and currently in pre‑alpha. Core mechanics work, but APIs and internals may change without notice.

---

### Index


- [What is cyberbrain?](#what-is-cyberbrain)
- [Architecture](#architecture)
- [Key features (current)](#key-features-current)
- [How it can be used](#how-it-can-be-used)
- [Documentation](#documentation)
    - [Getting Started](docs/GETTING_STARTED.md)
    - [Core Concepts & Architecture](docs/CONCEPTS.md)
    - [Developing Custom Actions](docs/ACTIONS.md)
- [Future plans](#future-plans)
- [License](#license)

---

### What is cyberbrain?

cyberbrain manages and processes information as a typed **knowledge graph**. It uses a decentralized, reactive approach to identify new or changed data (deltas), overlay declarative dependency patterns onto the stored graph, and schedule idempotent **Jobs** for registered **Actions**. Neurons (workers) execute jobs, map results back into the graph, and immediately trigger further scheduling — enabling iterative enrichment and automation. Neurons run multithreaded (configurable pool), which provides automated parallel processing of jobs by default.

---

### Architecture

**High-level overview**

![Cyberbrain overview](diagrams/overview.svg)

More visuals with brief captions are in [docs/DIAGRAMS.md](docs/DIAGRAMS.md).

---

### Key features (current)

* Graph memory: backed by **gits** (graph storage) for all entities, relations, and system state.
* Overlay-only scheduler: alias-aware pattern matching over the graph, seeded by batch anchors; strict delta causality to avoid false triggers.
* Idempotency without a global index: local, anchor-sharded Memory/Witness nodes prevent duplicate job scheduling.
* Decentralized execution: **Neurons** independently pick, execute, and feed results back; at least one neuron must be running.
* Pluggable **Actions**: implement domain logic; results are mapped using clear ID semantics (−1 create, −2 match by Type+Value, 0 parent+Value).

---

### How it can be used

Use cyberbrain wherever you want automated, iterative enrichment and orchestration driven by relationships in your data, for example:

* Data and asset discovery, stitching sources into a coherent graph and enriching nodes over time
* Continuous data enrichment and transformation pipelines based on declarative dependencies
* Event-driven automation in infrastructure, security, or observability domains (scan → map → act → repeat)
* Knowledge graph workflows where new relations or properties should automatically trigger further processing

---

### Documentation

* [Getting Started](docs/GETTING_STARTED.md)
* [Core Concepts & Architecture](docs/CONCEPTS.md)
* [Developing Custom Actions](docs/ACTIONS.md)

---

### Future plans

As mentioned in the disclaimer right now this project is in an early (experimental) stage. The following list is a collection of ideas and may change at any time:

* Implement a cyberbrain setting which extends the use of memory (witness) nodes to increase the traceability of your executions. 
* Add an example project consisting of multiple (~3) actions to showcase a simple use case and provide a reference implementation.
* Extend cyberbrain to support the definition of "whitelist" rules. These should be used to limit certain actions to certain domains (for example preventing actions on sensitive data).
* Add a simple tool to detect and visualize cyberbrain execution "paths" (e.g. a graph input -> actions -> outputs -> inputs ....).
* Add a "sense of time" to cyberbrain, which would allow to schedule jobs based on the passage of time.
* Add helpers to easier fetch and process final result data from cyberbrain

---

### License

cyberbrain is open-source software licensed under the [Mozilla Public License Version 2.0](LICENSE).