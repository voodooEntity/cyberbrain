package cerebrum

import "github.com/voodooEntity/gits"

// Activity structure contains all the main components of the scheduler
type Activity struct {
	Demultiplexer *Demultiplexer
	Scheduler     *Scheduler
}

// Consciousness structure contains all the main components of the cerebrum
type Consciousness struct {
	Memory   *Memory
	Cortex   *Cortex
	Activity *Activity
}

// Memory structure contains the main gits instance and mapper instance
// to group all "memory" related instances together
type Memory struct {
	Gits   *gits.Gits
	Mapper *Mapper
}

// Compiled dependency pattern structures (read-only, cached)
// PatternNode represents a single node in a compiled dependency tree,
// capturing alias (optional), type name, mode, filters and ordered children.
type PatternNode struct {
	Alias    string
	Type     string
	Mode     string
	Filters  map[string][3]string // key -> [Field, Operator, Value]
	Children []*PatternNode
	// NormalizedFilterFields contains derived keys for diagnostics, e.g.
	// Properties.Transport -> Transport
	NormalizedFilterFields map[string]string
}
