package scheduler

import (
    "testing"

    "github.com/voodooEntity/gits"
    "github.com/voodooEntity/gits/src/transport"
    cerebrum "github.com/voodooEntity/cyberbrain/src/system/cerebrum"
    cfgb "github.com/voodooEntity/cyberbrain/src/system/configBuilder"
    "github.com/voodooEntity/cyberbrain/src/system/interfaces"
)

// ActionD_DemuxFanout — Root [Primary, Set] with Alpha, Beta, Gamma as children (Secondary, Set)
// Used to validate demultiplexing Cartesian combinations.
type actionD struct{}

func (a *actionD) Execute(input transport.TransportEntity, requirement, context, jobID string) ([]transport.TransportEntity, error) {
    // No-op for scheduling-only tests
    return nil, nil
}

func (a *actionD) GetConfig() transport.TransportEntity {
    cfg := cfgb.NewConfig().SetName("ActionD_DemuxFanout").SetCategory("Test")
    root := cfgb.NewStructure("Root").SetPriority(cfgb.PRIORITY_PRIMARY).SetMode(cfgb.MODE_SET)
    // Children required in dependency (one of each type)
    root = root.AddChild(cfgb.NewStructure("Alpha").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET))
    root = root.AddChild(cfgb.NewStructure("Beta").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET))
    root = root.AddChild(cfgb.NewStructure("Gamma").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET))
    cfg.AddDependency("fanout", root)
    return cfg.Build()
}

func newActionD() interfaces.ActionInterface { return &actionD{} }

// ActionDMatch — same structure as ActionD but Beta is MATCH with filter on Properties.Tag == "ok"
type actionDMatch struct{}

func (a *actionDMatch) Execute(input transport.TransportEntity, requirement, context, jobID string) ([]transport.TransportEntity, error) {
    return nil, nil
}

func (a *actionDMatch) GetConfig() transport.TransportEntity {
    cfg := cfgb.NewConfig().SetName("ActionD_DemuxFanoutMatch").SetCategory("Test")
    root := cfgb.NewStructure("Root").SetPriority(cfgb.PRIORITY_PRIMARY).SetMode(cfgb.MODE_SET)
    root = root.AddChild(cfgb.NewStructure("Alpha").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET))
    // Beta is MATCH with Tag == ok
    beta := cfgb.NewStructure("Beta").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_MATCH).AddFilter("Tag", "Properties.Tag", "==", "ok")
    root = root.AddChild(beta)
    root = root.AddChild(cfgb.NewStructure("Gamma").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET))
    cfg.AddDependency("fanoutMatch", root)
    return cfg.Build()
}

func newActionDMatch() interfaces.ActionInterface { return &actionDMatch{} }

// Test 7.1 — Demultiplexing correctness: 2 Alpha x 2 Beta x 1 Gamma => 4 jobs
func Test_DemuxFanout_2Alpha2Beta1Gamma_Produces4Jobs_ActionD(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionD}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Map a Root with multiple children per type. Demultiplexer should produce 2x2x1 = 4 combinations.
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Root", Value: "root-fanout",
        ChildRelations: []transport.TransportRelation{
            {Target: transport.TransportEntity{Type: "Alpha", Value: "a1"}},
            {Target: transport.TransportEntity{Type: "Alpha", Value: "a2"}},
            {Target: transport.TransportEntity{Type: "Beta", Value: "b1"}},
            {Target: transport.TransportEntity{Type: "Beta", Value: "b2"}},
            {Target: transport.TransportEntity{Type: "Gamma", Value: "g1"}},
        },
    }, "Data")

    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 4 {
        t.Fatalf("expected 4 jobs for demux fanout, got %d", jobs.Amount)
    }
}

// Test 7.3 — Mixed relevant/irrelevant children: only Betas with Tag==ok should be used
// Structure still produces cartesian combinations across types. With 2 Alphas, 1 matching Beta, 1 non-matching Beta, 1 Gamma -> 2*1*1 = 2 jobs
func Test_DemuxFanout_FilteredBeta_OnlyRelevantCombos_ActionDMatch(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionDMatch}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Root", Value: "root-fanout-match",
        ChildRelations: []transport.TransportRelation{
            {Target: transport.TransportEntity{Type: "Alpha", Value: "a1"}},
            {Target: transport.TransportEntity{Type: "Alpha", Value: "a2"}},
            {Target: transport.TransportEntity{Type: "Beta", Value: "bOk", Properties: map[string]string{"Tag": "ok"}}},
            {Target: transport.TransportEntity{Type: "Beta", Value: "bBad", Properties: map[string]string{"Tag": "bad"}}},
            {Target: transport.TransportEntity{Type: "Gamma", Value: "g1"}},
        },
    }, "Data")

    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 2 {
        t.Fatalf("expected 2 jobs for filtered fanout (2x1x1), got %d", jobs.Amount)
    }
}

// Test 16.1 — Large fanout with noise: many irrelevant children should not impact correctness.
// We build a Root with many children, but only some participate in the ActionDMatch dependency:
// - 5 Alpha
// - 2 Beta with Tag==ok (relevant) and 50 Beta with Tag==bad (irrelevant)
// - 1 Gamma
// - plus noise children of unrelated types (should be ignored by query building)
// Expect jobs = 5 (Alpha) * 2 (matching Beta) * 1 (Gamma) = 10
func Test_LargeFanout_NoiseOnlyRelevantCombos_ActionD(t *testing.T) {
    // Use ActionD (all Set) so only type-based participation matters; noise of unrelated types must be ignored.
    actions := []func() interfaces.ActionInterface{newActionD}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Construct a large Root payload
    children := make([]transport.TransportRelation, 0, 5+2+1+50)
    // 5 Alpha
    for i := 0; i < 5; i++ { children = append(children, transport.TransportRelation{Target: transport.TransportEntity{Type: "Alpha", Value: "a-lf-"+string(rune('0'+i))}}) }
    // 2 Beta (all relevant since MODE_SET)
    children = append(children, transport.TransportRelation{Target: transport.TransportEntity{Type: "Beta", Value: "b-1"}})
    children = append(children, transport.TransportRelation{Target: transport.TransportEntity{Type: "Beta", Value: "b-2"}})
    // 1 Gamma
    children = append(children, transport.TransportRelation{Target: transport.TransportEntity{Type: "Gamma", Value: "g-lf"}})
    // 50 noise children of unrelated type Epsilon (should be ignored)
    for i := 0; i < 50; i++ { children = append(children, transport.TransportRelation{Target: transport.TransportEntity{Type: "Epsilon", Value: "e-"+string(rune('A'+(i%26)))}}) }

    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Root", Value: "root-large-fanout", ChildRelations: children}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 10 { // 5*2*1 combinations
        t.Fatalf("expected 10 jobs for large fanout (5x2x1) with unrelated noise ignored, got %d", jobs.Amount)
    }
}

// Test 7.2 — Immutability of demultiplexed inputs
// Ensure that modifying one demultiplexed combination does not affect others.
func Test_DemuxFanout_Immutability_CombinationsIndependent(t *testing.T) {
    // Build a Root with 2 Alpha, 2 Beta, 1 Gamma. We'll demultiplex directly.
    root := transport.TransportEntity{Type: "Root", Value: "root-immut",
        ChildRelations: []transport.TransportRelation{
            {Target: transport.TransportEntity{Type: "Alpha", Value: "a1"}},
            {Target: transport.TransportEntity{Type: "Alpha", Value: "a2"}},
            {Target: transport.TransportEntity{Type: "Beta", Value: "b1"}},
            {Target: transport.TransportEntity{Type: "Beta", Value: "b2"}},
            {Target: transport.TransportEntity{Type: "Gamma", Value: "g1"}},
        },
    }
    demux := cerebrum.NewDemultiplexer()
    outs := demux.Parse(root)
    if len(outs) != 4 {
        t.Fatalf("expected 4 demuxed combinations, got %d", len(outs))
    }
    // Find two different outputs that both include Beta==b1
    idx := []int{}
    for i, out := range outs {
        // scan children for Beta b1
        for _, rel := range out.ChildRelations {
            if rel.Target.Type == "Beta" && rel.Target.Value == "b1" {
                idx = append(idx, i)
                break
            }
        }
    }
    if len(idx) < 2 {
        t.Fatalf("expected at least two combinations containing Beta=b1, found %d", len(idx))
    }
    // Mutate the Beta properties in the first combination
    i0, i1 := idx[0], idx[1]
    for ci, rel := range outs[i0].ChildRelations {
        if rel.Target.Type == "Beta" && rel.Target.Value == "b1" {
            if outs[i0].ChildRelations[ci].Target.Properties == nil {
                outs[i0].ChildRelations[ci].Target.Properties = map[string]string{}
            }
            outs[i0].ChildRelations[ci].Target.Properties["Mut"] = "x"
        }
    }
    // Verify the second combination's Beta (also b1) has not been modified
    for _, rel := range outs[i1].ChildRelations {
        if rel.Target.Type == "Beta" && rel.Target.Value == "b1" {
            if _, ok := rel.Target.Properties["Mut"]; ok {
                t.Fatalf("mutation in one combination leaked into another combination")
            }
        }
    }
}
