package scheduler

import (
    "testing"

    "github.com/voodooEntity/gits"
    "github.com/voodooEntity/gits/src/transport"
    cfgb "github.com/voodooEntity/cyberbrain/src/system/configBuilder"
    "github.com/voodooEntity/cyberbrain/src/system/interfaces"
)

// ActionRelChild — simple parent/child structure to validate relation-only causality guard.
// Dependency: Bucket [Secondary, Set] -> Item [Primary, Set]
type actionRelChild struct{}

func (a *actionRelChild) Execute(input transport.TransportEntity, requirement, context, jobID string) ([]transport.TransportEntity, error) {
    return nil, nil
}

func (a *actionRelChild) GetConfig() transport.TransportEntity {
    cfg := cfgb.NewConfig().SetName("ActionRelChild").SetCategory("Test")
    dep := cfgb.NewStructure("Bucket").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET).AddChild(
        cfgb.NewStructure("Item").SetPriority(cfgb.PRIORITY_PRIMARY).SetMode(cfgb.MODE_SET),
    )
    cfg.AddDependency("bucketItem", dep)
    return cfg.Build()
}

func newActionRelChild() interfaces.ActionInterface { return &actionRelChild{} }

// Test: Adding a new child under the same parent should not reschedule the old child combination.
// Steps:
//  1) Seed a Bucket only (no job yet as we don't schedule on seed here).
//  2) Delta: add Item#1 under Bucket → expect 1 job.
//  3) Delta: add Item#2 under same Bucket → expect total 2 jobs (Item#1 must NOT be rescheduled).
func Test_RelationOnlySiblingGuard_NoRescheduleOldChild(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionRelChild}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed a Bucket
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Bucket", Value: "B-1"}, "Data")

    // Lookup Bucket ID
    rB := mem.Gits.Query().Execute(gits.NewQuery().Read("Bucket").Match("Value", "==", "B-1"))
    if rB.Amount == 0 { t.Fatalf("Bucket not found after seed") }
    bucketID := rB.Entities[0].ID

    // Delta #1: add Item#1 under Bucket
    mapped1 := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Bucket", ID: bucketID,
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Item", Value: "I-1"}}},
    }, "Data")
    sched.Run(mapped1, cortex)

    jobs1 := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs1.Amount != 1 {
        t.Fatalf("expected 1 job after first child, got %d", jobs1.Amount)
    }

    // Delta #2: add Item#2 under the same Bucket. Previously, sibling cross-trigger could reschedule Item#1.
    mapped2 := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Bucket", ID: bucketID,
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Item", Value: "I-2"}}},
    }, "Data")
    sched.Run(mapped2, cortex)

    jobs2 := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs2.Amount != 2 {
        t.Fatalf("expected total 2 jobs after adding a second child (no reschedule of first), got %d", jobs2.Amount)
    }
}

// Test: Multiple new children in one delta should schedule once per new child, without touching existing ones.
// Steps:
//  1) Seed Bucket and add Item#1 (1 job expected after scheduling).
//  2) In a single delta, add Item#2 and Item#3 under the same Bucket.
//  3) Expect total 3 jobs (existing Item#1 not rescheduled).
func Test_RelationOnly_MultipleNewChildren_OnlyNewOnesScheduled(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionRelChild}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed a Bucket
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Bucket", Value: "B-2"}, "Data")
    rB := mem.Gits.Query().Execute(gits.NewQuery().Read("Bucket").Match("Value", "==", "B-2"))
    if rB.Amount == 0 { t.Fatalf("Bucket not found after seed") }
    bucketID := rB.Entities[0].ID

    // First child Item#1
    mapped1 := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Bucket", ID: bucketID,
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Item", Value: "I-1"}}},
    }, "Data")
    sched.Run(mapped1, cortex)

    jobs1 := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs1.Amount != 1 {
        t.Fatalf("expected 1 job after first child, got %d", jobs1.Amount)
    }

    // Second delta: add two new children at once
    mapped2 := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Bucket", ID: bucketID,
        ChildRelations: []transport.TransportRelation{
            {Target: transport.TransportEntity{Type: "Item", Value: "I-2"}},
            {Target: transport.TransportEntity{Type: "Item", Value: "I-3"}},
        },
    }, "Data")
    sched.Run(mapped2, cortex)

    jobs2 := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs2.Amount != 3 {
        t.Fatalf("expected total 3 jobs after adding two more children, got %d", jobs2.Amount)
    }
}
