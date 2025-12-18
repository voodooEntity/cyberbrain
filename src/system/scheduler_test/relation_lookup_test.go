package scheduler

import (
    "testing"

    "github.com/voodooEntity/gits"
    "github.com/voodooEntity/gits/src/transport"
    cfgb "github.com/voodooEntity/cyberbrain/src/system/configBuilder"
    "github.com/voodooEntity/cyberbrain/src/system/interfaces"
)

// Test 11.1 — Relation structure lookup (DependencyRelationLookup)
// Ensure that adding an Alpha→Beta edge triggers scheduling for an action
// indexed by that relation structure when the rest of the path exists.
func Test_RelationStructureLookup_AlphaToBeta_Triggers_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed Beta->Gamma->Delta (filters satisfied at Delta)
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type:  "Beta",
        Value: "b-rs-lookup",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Gamma",
            Value: "g-rs-lookup",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:       "Delta",
                Value:      "protoX",
                Properties: map[string]string{"Transport": "secure"},
            }}},
        }}},
    }, "Data")

    // Lookup Beta ID
    rB := mem.Gits.Query().Execute(gits.NewQuery().Read("Beta").Match("Value", "==", "b-rs-lookup"))
    if rB.Amount == 0 { t.Fatalf("Beta not found for relation structure lookup") }
    betaID := rB.Entities[0].ID

    // Delta: relation-only Alpha→Beta (this adds relation structure Alpha-Beta and should trigger via relation lookup)
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "a-rs-lookup",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Beta", ID: betaID}}},
    }, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job after relation structure Alpha→Beta, got %d", jobs.Amount)
    }
}

// actionG — simple Primary MODE_SET on Beta to test DependencyEntityLookup directly
type actionG struct{}

func (a *actionG) Execute(input transport.TransportEntity, requirement, context, jobID string) ([]transport.TransportEntity, error) {
    return nil, nil
}

func (a *actionG) GetConfig() transport.TransportEntity {
    cfg := cfgb.NewConfig().SetName("ActionG_PrimaryBeta").SetCategory("Test")
    dep := cfgb.NewStructure("Beta").SetPriority(cfgb.PRIORITY_PRIMARY).SetMode(cfgb.MODE_SET)
    cfg.AddDependency("betaOnly", dep)
    return cfg.Build()
}

func newActionG() interfaces.ActionInterface { return &actionG{} }

// Test 11.2 — Entity-type lookup (DependencyEntityLookup)
// Creating the Primary entity for a MODE_SET dependency should schedule a job even without relations.
func Test_EntityTypeLookup_PrimaryBetaCreation_Triggers_ActionG(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionG}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Delta: create Beta (Primary, Set)
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Beta", Value: "b-entity-lookup"}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected 1 job for Primary Beta creation via entity-type lookup, got %d", jobs.Amount)
    }
    if jobs.Entities[0].Properties["Requirement"] != "betaOnly" {
        t.Fatalf("expected Requirement=betaOnly, got %s", jobs.Entities[0].Properties["Requirement"])
    }
}
