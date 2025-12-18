package scheduler

import (
    "testing"

    "github.com/voodooEntity/gits"
    "github.com/voodooEntity/gits/src/transport"
    cfgb "github.com/voodooEntity/cyberbrain/src/system/configBuilder"
    "github.com/voodooEntity/cyberbrain/src/system/interfaces"
)

// ActionE_MultiDep — single action registering two dependencies
// - shallow: Alpha [Primary, Set]
// - deep: Alpha -> Beta -> Gamma -> Delta [Primary, Match]{Value==protoX, Properties.Transport==secure}
type actionE struct{}

func (a *actionE) Execute(input transport.TransportEntity, requirement, context, jobID string) ([]transport.TransportEntity, error) {
    return nil, nil
}

func (a *actionE) GetConfig() transport.TransportEntity {
    cfg := cfgb.NewConfig().SetName("ActionE_MultiDep").SetCategory("Test")
    // shallow dependency
    shallow := cfgb.NewStructure("Alpha").SetPriority(cfgb.PRIORITY_PRIMARY).SetMode(cfgb.MODE_SET)
    cfg.AddDependency("shallow", shallow)

    // deep dependency
    deep := cfgb.NewStructure("Alpha").AddChild(
        cfgb.NewStructure("Beta").AddChild(
            cfgb.NewStructure("Gamma").AddChild(
                cfgb.NewStructure("Delta").SetPriority(cfgb.PRIORITY_PRIMARY).SetMode(cfgb.MODE_MATCH).
                    AddFilter("Protocol", "Value", "==", "protoX").
                    AddFilter("Transport", "Properties.Transport", "==", "secure"),
            ),
        ),
    )
    cfg.AddDependency("deep", deep)
    return cfg.Build()
}

func newActionE() interfaces.ActionInterface { return &actionE{} }

// Test 8.1 — Multiple dependencies per action; satisfy only shallow, expect one job for that dependency
func Test_MultiDependency_OnlyShallowSatisfied_SchedulesOneJob(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionE}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Trigger delta: create Alpha only (satisfies shallow but not deep)
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "a-md1"}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected 1 job when only shallow dependency is satisfied, got %d", jobs.Amount)
    }
    // Verify the job is for the 'shallow' requirement
    if jobs.Entities[0].Properties["Requirement"] != "shallow" {
        t.Fatalf("expected Requirement=shallow, got %s", jobs.Entities[0].Properties["Requirement"])
    }
}

// Test 8.2 — Satisfy both dependencies in separate deltas → expect two jobs (one per dependency), no cross-duplication
func Test_MultiDependency_SatisfyBothDependencies_TwoJobs(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionE}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // First delta: create Alpha only (satisfies shallow)
    mapped1 := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "a-md2"}, "Data")
    sched.Run(mapped1, cortex)

    // Assert one job exists with Requirement=shallow
    jobs1 := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs1.Amount != 1 {
        t.Fatalf("expected 1 job after shallow delta, got %d", jobs1.Amount)
    }
    if jobs1.Entities[0].Properties["Requirement"] != "shallow" {
        t.Fatalf("expected first job Requirement=shallow, got %s", jobs1.Entities[0].Properties["Requirement"])
    }

    // Seed the remaining chain under the same Alpha to satisfy the deep dependency.
    // Find Alpha ID to attach the rest.
    rA := mem.Gits.Query().Execute(gits.NewQuery().Read("Alpha").Match("Value", "==", "a-md2"))
    if rA.Amount == 0 {
        t.Fatalf("Alpha not found")
    }
    alphaID := rA.Entities[0].ID

    // Map Beta->Gamma->Delta with filters matching (Value=protoX, Transport=secure)
    mapped2 := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type: "Alpha", ID: alphaID,
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type: "Beta", Value: "b-md2",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type: "Gamma", Value: "g-md2",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type: "Delta", Value: "protoX", Properties: map[string]string{"Transport": "secure"},
                }}},
            }}},
        }}},
    }, "Data")
    sched.Run(mapped2, cortex)

    // Expect now two jobs in total: one for shallow, one for deep
    jobs2 := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs2.Amount != 2 {
        t.Fatalf("expected 2 jobs after satisfying both dependencies, got %d", jobs2.Amount)
    }

    // Verify we have one shallow and one deep requirement present
    reqs := map[string]int{}
    for _, e := range jobs2.Entities {
        reqs[e.Properties["Requirement"]]++
    }
    if reqs["shallow"] != 1 || reqs["deep"] != 1 {
        t.Fatalf("expected one shallow and one deep job, got counts: %+v", reqs)
    }

    // Ensure no duplication: re-running a no-op delta (e.g., linking Alpha to existing Beta again should not create new relation)
    // Try to map relation-only Alpha->Beta again and run scheduler; relations already exist so created relation won't be marked with bMap.
    // Lookup Beta ID
    rB := mem.Gits.Query().Execute(gits.NewQuery().Read("Beta").Match("Value", "==", "b-md2"))
    if rB.Amount == 0 { t.Fatalf("Beta not found") }
    betaID := rB.Entities[0].ID
    noop := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", ID: alphaID,
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Beta", ID: betaID}}},
    }, "Data")
    sched.Run(noop, cortex)
    jobs3 := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs3.Amount != 2 {
        t.Fatalf("expected still 2 jobs after no-op relation mapping, got %d", jobs3.Amount)
    }
}
