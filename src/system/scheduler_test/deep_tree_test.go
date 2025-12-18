package scheduler

import (
    "testing"

    "github.com/voodooEntity/gits"
    "github.com/voodooEntity/gits/src/transport"
    cfgb "github.com/voodooEntity/cyberbrain/src/system/configBuilder"
    "github.com/voodooEntity/cyberbrain/src/system/interfaces"
)

// ActionH_DeepFive — deep 5-level dependency to validate traversal/query building
// Dependency: Alpha [Secondary, Set] -> Beta [Secondary, Set] -> Gamma [Secondary, Set] -> Delta [Secondary, Set] -> Epsilon [Primary, Set]
type actionH struct{}

func (a *actionH) Execute(input transport.TransportEntity, requirement, context, jobID string) ([]transport.TransportEntity, error) {
    return nil, nil
}

func (a *actionH) GetConfig() transport.TransportEntity {
    cfg := cfgb.NewConfig().SetName("ActionH_DeepFive").SetCategory("Test")
    dep := cfgb.NewStructure("Alpha").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET).AddChild(
        cfgb.NewStructure("Beta").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET).AddChild(
            cfgb.NewStructure("Gamma").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET).AddChild(
                cfgb.NewStructure("Delta").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET).AddChild(
                    cfgb.NewStructure("Epsilon").SetPriority(cfgb.PRIORITY_PRIMARY).SetMode(cfgb.MODE_SET),
                ),
            ),
        ),
    )
    cfg.AddDependency("deep5", dep)
    return cfg.Build()
}

func newActionH() interfaces.ActionInterface { return &actionH{} }

// Test 16.2 — Deep trees up to 5 levels: verify traversal and query building produce correct inputs
// Seed Alpha->Beta->Gamma->Delta (no Epsilon). Delta adds Epsilon under Delta in one delta.
// Expect exactly one job for ActionH when Epsilon is added.
func Test_DeepFive_AddEpsilon_TriggersOnce_ActionH(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionH}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed the first four levels: Alpha -> Beta -> Gamma -> Delta
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-df",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-df",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-df",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:  "Delta",
                    Value: "d-df",
                }}},
            }}},
        }}},
    }, "Data")

    // Lookup Delta ID to attach Epsilon child via delta
    rD := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "d-df"))
    if rD.Amount == 0 { t.Fatalf("Delta not found for deep tree test") }
    deltaID := rD.Entities[0].ID

    // Delta: add Epsilon [Primary, Set] under Delta
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type: "Delta", ID: deltaID,
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Epsilon", Value: "e-df"}}},
    }, "Data")

    // Run scheduler and expect exactly one job
    sched.Run(mapped, cortex)
    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job for deep 5-level dependency, got %d", jobs.Amount)
    }
}
