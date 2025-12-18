# Developing Custom Actions for cyberbrain

Actions are the pluggable units that do work in cyberbrain. You implement the
ActionInterface, declare a declarative dependency graph for each action, and the
scheduler will invoke your action when its dependency is satisfied. Your action
returns transport entities that the Mapper integrates back into the graph.

---

## ActionInterface (current)

Every action must implement the interface below:

```go
type ActionInterface interface {
    // input: the constructed job payload (graph slice) that matched the dependency
    // requirement: the dependency name that matched (for actions with multiple deps)
    // context: free-form, usually carried from inputs (not used for scheduling)
    // jobID: persistent Job identifier (string)
    Execute(input transport.TransportEntity, requirement, context, jobID string) ([]transport.TransportEntity, error)
    // Declarative definition of the dependency structure for this action
    GetConfig() transport.TransportEntity
}
```

Notes:
- The neuron injects optional dependencies if your action also implements
  SetGits/SetMapper/SetLogger (see “Dependency injection” below).
- Execute must be idempotent with respect to the graph: prefer reusing existing
  anchors via identity (see ID semantics) to avoid duplicates.

---

## Building dependencies with configBuilder

Use the fluent configBuilder to describe the dependency tree and filters. Below
are two concrete examples: a minimal 3‑level tree and a more advanced one that
uses aliases for same‑type siblings.

For context on how dependencies participate in matching and job creation, see the scheduling flow:

![Scheduling flow](../diagrams/scheduling_flow.svg)

Key concepts:
- Priority: Primary vs Secondary — concrete scheduling semantics:
  - Primary nodes are entry points. The appearance/update of a Primary node (or a relation‑only delta that includes the Primary as the updated child) can independently seed scheduling for the dependency. Think of Primary as "may trigger by itself".
  - Secondary nodes participate in the match but do not independently trigger scheduling. They help complete patterns when a causally updated entity exists somewhere in the constructed input (strict causality applies). Adding/updating a Secondary can still result in a job if that Secondary is itself the updated child in a relation‑only delta or carries a relevant property update and the full pattern can be formed.
  - Multiple Primaries: if a dependency tree contains more than one Primary along the path, any causally updated Primary can serve as the anchor for matching. The scheduler picks a deterministic anchor for witness/idempotency, but triggering can start from any Primary affected in the batch.
  - Guidance: choose Primary for nodes you expect to drive the workflow when they appear (e.g., IP, Port, Vhost, Directory roots). Mark the rest as Secondary to have them required for matching without making every change on them a standalone trigger.
- Mode: Set vs Match — Match nodes can have filters (on entity properties) and only schedule when a relevant updated key matches the filter; Set nodes are structure/presence‑based.
- Aliases: assign `Alias` to distinguish multiple siblings of the same Type.

How demultiplexing across alias slots works:

![Demultiplexing across alias slots](../diagrams/demux_slots.svg)

Example A — simple, easy to grasp (3 levels):

```go
cfg := configBuilder.NewConfig().SetName("ActionSimple").SetCategory("Demo")

// Alpha [Primary, Set] → Beta [Secondary, Set] → Gamma [Secondary, Match]{Transport==secure}
alpha := configBuilder.NewStructure("Alpha").
    SetPriority(configBuilder.PRIORITY_PRIMARY).
    SetMode(configBuilder.MODE_SET)

beta := configBuilder.NewStructure("Beta").
    SetPriority(configBuilder.PRIORITY_SECONDARY).
    SetMode(configBuilder.MODE_SET)

gamma := configBuilder.NewStructure("Gamma").
    SetPriority(configBuilder.PRIORITY_SECONDARY).
    SetMode(configBuilder.MODE_MATCH).
    AddFilter("Transport", "Properties.Transport", "==", "secure")

alpha.AddChild(beta)
beta.AddChild(gamma)

cfg.AddDependency("alphaBetaGamma", alpha)
return cfg.Build()
```

Example B — advanced, using aliases for same‑type siblings:

```go
cfg := configBuilder.NewConfig().SetName("ActionWithAliases").SetCategory("Demo")

// Root [Primary, Set] → Item[left] [Secondary, Set] & Item[right] [Secondary, Set]
root := configBuilder.NewStructure("Root").
    SetPriority(configBuilder.PRIORITY_PRIMARY).
    SetMode(configBuilder.MODE_SET)

left := configBuilder.NewStructure("Item").
    SetAlias("left").
    SetPriority(configBuilder.PRIORITY_SECONDARY).
    SetMode(configBuilder.MODE_SET)

right := configBuilder.NewStructure("Item").
    SetAlias("right").
    SetPriority(configBuilder.PRIORITY_SECONDARY).
    SetMode(configBuilder.MODE_SET)

root.AddChild(left).AddChild(right)

cfg.AddDependency("rootWithItems", root)
return cfg.Build()
```

The scheduler compiles and caches dependencies as alias‑aware patterns and
matches them against newly mapped deltas.

Tip — how Priority affects these examples:
- In Example A, Alpha is Primary: creating Alpha (or a relation‑only child under Alpha) can trigger matching. Beta and Gamma are Secondary: they must exist/match, but changes to them alone won't trigger unless they are the causally updated child in the batch and the full pattern can be built.
- In Example B, Root is Primary and the two Item children are Secondary with aliases: creating Root (or one of the Items as the updated child) can trigger; the matcher fills both alias slots before scheduling.

---

## Execute: return results to be mapped

Your Execute should read the input payload, perform domain work, and return one
or more transport entities (graphs) to be mapped by the Mapper.

Practical rules for IDs (Mapper identity semantics):
- `ID = -1`: force create new entity.
- `ID = -2`: match by (Type, Value) or create if absent. Use for global anchors
  (e.g., IP, Domain) to reuse existing nodes across parents.
- `ID = 0`: try parents + (Type & Value) match else create (scoped to the related parent).

Referencing existing entities by Type+ID (anchoring and linking):

- If you include entities with existing `(Type, ID)` in your returned graph, the
  mapper will map onto those existing entities. This lets you anchor your output
  directly on input data (e.g., reuse the exact Port/Directory you received).
- You can also create relations between two existing entities by nesting them in
  the return structure with their known `(Type, ID)`; no new nodes are created,
  only the relation is added if missing.

Example patterns (pseudocode snippets):

```go
// 1) Reuse existing IP by value when creating a new Domain→IP edge (redirects)
ret := transport.TransportEntity{ ID:-2, Type:"Domain", Value:host, Context:jobUUID,
  ChildRelations: []transport.TransportRelation{{
    Target: transport.TransportEntity{ ID:-2, Type:"IP", Value:ipv4, Context:jobUUID,
      ChildRelations: []transport.TransportRelation{{ Target: portEntity }},
    },
  }},
}

// 2) Force-create a Directory under an existing Vhost
dir := transport.TransportEntity{ ID:-1, Type:"Directory", Value:"/admin" }
vhost.ChildRelations = append(vhost.ChildRelations, transport.TransportRelation{ Target: dir })
return []transport.TransportEntity{ vhost }, nil

// 3) Anchor on an existing Port and link to an existing Vhost (create relation only)
existingPort := transport.TransportEntity{ ID:42, Type:"Port" }       // already in storage
existingVhost := transport.TransportEntity{ ID:7, Type:"Vhost" }      // already in storage
existingPort.ChildRelations = append(existingPort.ChildRelations, transport.TransportRelation{ Target: existingVhost })
return []transport.TransportEntity{ existingPort }, nil
```

The neuron maps the returned graphs, then immediately re‑runs scheduling on the
freshly mapped delta.

---

## Dependency injection (optional)

If your action type defines any of these methods, the neuron will inject the
corresponding instances before Execute:

- `SetGits(g *gits.Gits)` — direct graph access (rarely needed)
- `SetMapper(m *cerebrum.Mapper)` — call Mapper directly (usually not needed)
- `SetLogger(l *archivist.Archivist)` — structured logging within your action

Keep side‑effects minimal. Most actions should just return transport entities
and let the neuron/mapper handle persistence.

---

## Registration

Register actions before starting the system:

```go
cb := cyberbrain.New(cyberbrain.Settings{ Ident:"my-run", NeuronAmount:1 })
cb.RegisterAction("myAction", myActionFactory)
cb.Start()
```

The dependency name used in `AddDependency("…", …)` will appear as the
`requirement` parameter in Execute.

---

## Scheduler behavior (what to expect)

- Overlay‑only matching: pattern is overlaid on storage starting from batch
  anchors (entities with bMap; relation‑only child endpoints). Aliases allow
  same‑type siblings.
- Strict causality: a constructed input is accepted only if it contains at least
  one updated entity from this batch; for relation‑only deltas, the updated
  child must be included (a shared parent alone is insufficient).
- Idempotency: before creating a Job, the scheduler creates/looks up a local
  Memory/Witness node (Context=`Exec:<Action>:<Dep>`, Value=`signatureHash`) off
  a deterministic anchor. If it exists, the job is skipped.

Action authors should avoid introducing duplicates by honoring ID semantics and
returning stable identity values (e.g., IP string); do not include per‑run salts
in identity fields.

---

## Tips & guardrails for action authors

- Always run the system with ≥ 1 neuron; tests may use the lightweight harness.
- Don’t rely on Context for scheduling — it’s not included in signatures.
- Prefer `ID:-2` for anchors (IP/Domain) so outputs merge by value.
- When adding many children (e.g., directories), ensure values are stable and
  dedupe at the source if appropriate.
- Keep logs concise; the scheduler emits TRACE logs prefixed with “scheduling”.

---

## Where to go next

- Read: Core concepts & architecture (docs/CONCEPTS.md)
- Try: Getting started (docs/GETTING_STARTED.md)

