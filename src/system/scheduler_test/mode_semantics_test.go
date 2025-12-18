package scheduler

import (
    "testing"

    "github.com/voodooEntity/gits"
    "github.com/voodooEntity/gits/src/transport"
    cfgb "github.com/voodooEntity/cyberbrain/src/system/configBuilder"
    "github.com/voodooEntity/cyberbrain/src/system/interfaces"
)

// ActionF_SetDeep — MODE_SET semantics over a small structure
// Dependency: Alpha [Secondary, Set] -> Beta [Primary, Set]
// Adding Beta under Alpha should trigger one job (9.1)
type actionF struct{}

func (a *actionF) Execute(input transport.TransportEntity, requirement, context, jobID string) ([]transport.TransportEntity, error) {
    return nil, nil
}

func (a *actionF) GetConfig() transport.TransportEntity {
    cfg := cfgb.NewConfig().SetName("ActionF_SetDeep").SetCategory("Test")
    dep := cfgb.NewStructure("Alpha").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET).AddChild(
        cfgb.NewStructure("Beta").SetPriority(cfgb.PRIORITY_PRIMARY).SetMode(cfgb.MODE_SET),
    )
    cfg.AddDependency("alphaBeta", dep)
    return cfg.Build()
}

func newActionF() interfaces.ActionInterface { return &actionF{} }

// Test 9.1 — MODE_SET: adding structure (entities/relations) with no filters should trigger once per new match.
// Seed Alpha; delta: add Beta under Alpha → expect exactly one job for ActionF.
func Test_ModeSet_AddStructure_TriggersOnce_ActionF(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionF}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed Alpha only (no job expected at seed-time since we don't run scheduler here)
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "a-f1"}, "Data")

    // Lookup Alpha ID
    rA := mem.Gits.Query().Execute(gits.NewQuery().Read("Alpha").Match("Value", "==", "a-f1"))
    if rA.Amount == 0 { t.Fatalf("Alpha not found") }
    alphaID := rA.Entities[0].ID

    // Delta: add Beta under Alpha (relation created should be marked by bMap)
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", ID: alphaID,
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Beta", Value: "b-f1"}}},
    }, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job for MODE_SET structure addition, got %d", jobs.Amount)
    }
}

// Test 9.2 — MODE_MATCH: only deltas that affect filter‑relevant fields should trigger; irrelevant updates must not.
// Use ActionB (deep MATCH on Delta). Seed a fully matching path once (no schedule at seed time), then
// update an unrelated property on Delta (not part of filters). Expect 0 jobs.
func Test_ModeMatch_IrrelevantDeltaPropertyUpdate_NoJob_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed full path where Delta already matches filters (Value=protoX, Transport=secure)
    seed := transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-m9-2",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-m9-2",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-m9-2",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoX",
                    Properties: map[string]string{"Transport": "secure"},
                }}},
            }}},
        }}},
    }
    _ = mem.Mapper.MapTransportDataWithContext(seed, "Data")

    // Find Delta and update a non-relevant property (e.g., Tag). Filters are on Value and Properties.Transport only.
    rD := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "protoX"))
    if rD.Amount == 0 { t.Fatalf("Delta not found") }
    deltaID := rD.Entities[0].ID

    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: deltaID, Properties: map[string]string{"Tag": "noop"}}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 0 {
        t.Fatalf("expected 0 jobs for irrelevant property update on MATCH node, got %d", jobs.Amount)
    }
}

// Test 9.3 — Mixed Set/Match in a tree: changing only Set nodes/structure should not trigger when Match node
// remains unsatisfied. We attach Alpha→Beta (Set structure change) while Delta does not satisfy filters.
func Test_MixedSetMatch_SetStructureChange_NoJob_WhileDeltaNotMatching(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed Beta->Gamma->Delta where Delta does NOT match (Transport=plain)
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type:  "Beta",
        Value: "b-m9-3",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Gamma",
            Value: "g-m9-3",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:       "Delta",
                Value:      "protoX",
                Properties: map[string]string{"Transport": "plain"},
            }}},
        }}},
    }, "Data")

    // Seed Alpha separately
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "a-m9-3"}, "Data")

    // Lookup Alpha and Beta IDs
    rA := mem.Gits.Query().Execute(gits.NewQuery().Read("Alpha").Match("Value", "==", "a-m9-3"))
    if rA.Amount == 0 { t.Fatalf("Alpha not found") }
    alphaID := rA.Entities[0].ID
    rB := mem.Gits.Query().Execute(gits.NewQuery().Read("Beta").Match("Value", "==", "b-m9-3"))
    if rB.Amount == 0 { t.Fatalf("Beta not found") }
    betaID := rB.Entities[0].ID

    // Delta: create relation Alpha→Beta (Set structure change). Since Delta still does not match filters,
    // no job should be scheduled.
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", ID: alphaID,
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Beta", ID: betaID}}},
    }, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 0 {
        t.Fatalf("expected 0 jobs when only Set nodes change and Match node remains unsatisfied, got %d", jobs.Amount)
    }
}
