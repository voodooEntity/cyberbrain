package scheduler

import (
    "testing"

    "github.com/voodooEntity/gits"
    "github.com/voodooEntity/gits/src/transport"
    cfgb "github.com/voodooEntity/cyberbrain/src/system/configBuilder"
    "github.com/voodooEntity/cyberbrain/src/system/interfaces"
)

// ActionA_SetPrimaryOnly — minimal mock action for tests
type actionA struct{}

// Execute implements interfaces.ActionInterface
// Signature: Execute(input, requirement, context, jobID)
func (a *actionA) Execute(input transport.TransportEntity, requirement string, context string, jobID string) ([]transport.TransportEntity, error) {
	// For scheduling-only test we don't need to produce outputs.
	return nil, nil
}

func (a *actionA) GetConfig() transport.TransportEntity {
	cfg := cfgb.NewConfig().SetName("ActionA_SetPrimaryOnly").SetCategory("Test")
	dep := cfgb.NewStructure("Alpha").SetPriority(cfgb.PRIORITY_PRIMARY).SetMode(cfgb.MODE_SET)
	cfg.AddDependency("alpha", dep)
	return cfg.Build()
}

func newActionA() interfaces.ActionInterface { return &actionA{} }

// Test 1.1 — Primary entity creation (MODE_SET): Create Alpha -> expect one job for ActionA
func Test_PrimaryEntityCreate_ModeSet_ActionA(t *testing.T) {
    // Use the lightweight scheduler harness
    actions := []func() interfaces.ActionInterface{newActionA}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Trigger delta: create Alpha in Data context so bMap is set
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "a1", Properties: map[string]string{}}, "Data")
    // Run scheduler directly
    sched.Run(mapped, cortex)

    // Assert: one Job exists in storage
    q := gits.NewQuery().Read("Job")
    res := mem.Gits.Query().Execute(q)
    if res.Amount != 1 {
        t.Fatalf("expected exactly 1 job after creating Alpha, got %d", res.Amount)
    }
}

// Test 1.2 — Duplicate primary creation should not create a second job
// Steps:
//  - Map Alpha once (creates entity, sets bMap=""), run scheduler → 1 Job
//  - Map the same Alpha again (merges into existing, sets bMap to updated keys or none), run scheduler → still 1 Job
func Test_PrimaryEntityDuplicate_NoSecondJob_ActionA(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionA}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // First creation
    mapped1 := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "dup-alpha", Properties: map[string]string{}}, "Data")
    sched.Run(mapped1, cortex)

    // Second mapping with same identity (no ID, same Value) — should not schedule another job
    mapped2 := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "dup-alpha", Properties: map[string]string{}}, "Data")
    sched.Run(mapped2, cortex)

    // Assert only a single job exists
    res := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if res.Amount != 1 {
        t.Fatalf("expected exactly 1 job after duplicate create, got %d", res.Amount)
    }
}

// Test 2.1 — Irrelevant property update on Primary node (no filters) should not schedule
// Dependency ActionA has no filters (MODE_SET). Updating an unrelated property on Alpha
// must not create a job.
func Test_IrrelevantPrimaryPropertyUpdate_NoJob_ActionA(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionA}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed Alpha
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "alpha-prop", Properties: map[string]string{}}, "Data")

    // Update an unrelated property on Alpha; since ActionA has no filters, scheduler should skip
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", ID: 0, Value: "alpha-prop", Properties: map[string]string{"Unrelated": "x"}}, "Data")
    sched.Run(mapped, cortex)

    // Assert: no jobs were created
    res := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if res.Amount != 0 {
        t.Fatalf("expected 0 jobs after irrelevant primary property update, got %d", res.Amount)
    }
}
