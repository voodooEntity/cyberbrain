package scheduler

import (
    "strconv"
    "sync"
    "testing"

    "github.com/voodooEntity/gits"
    "github.com/voodooEntity/gits/src/query"
    "github.com/voodooEntity/gits/src/transport"
    cfgb "github.com/voodooEntity/cyberbrain/src/system/configBuilder"
    "github.com/voodooEntity/cyberbrain/src/system/interfaces"
)

// actionB — deep MATCH dependency used for tests 2–4
type actionB struct{}

func (a *actionB) Execute(input transport.TransportEntity, requirement string, context string, jobID string) ([]transport.TransportEntity, error) {
	// No-op for scheduling tests
	return nil, nil
}

func (a *actionB) GetConfig() transport.TransportEntity {
	cfg := cfgb.NewConfig().SetName("ActionB_MatchDeep").SetCategory("Test")
	dep := cfgb.NewStructure("Alpha").AddChild(
		cfgb.NewStructure("Beta").AddChild(
			cfgb.NewStructure("Gamma").AddChild(
				cfgb.NewStructure("Delta").
					SetPriority(cfgb.PRIORITY_PRIMARY).
					SetMode(cfgb.MODE_MATCH).
					AddFilter("Protocol", "Value", "==", "protoX").
					AddFilter("Transport", "Properties.Transport", "==", "secure"),
			),
		),
	)
	cfg.AddDependency("deep", dep)
	return cfg.Build()
}

func newActionB() interfaces.ActionInterface { return &actionB{} }

// Test 2 — Filter-relevant property update on MATCH node triggers scheduling
// Seed Alpha->Beta->Gamma->Delta with Transport=plain, then update Transport=secure -> expect 1 Job
func Test_FilterRelevantUpdate_Triggers_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)
    // Seed full chain (no scheduling): Delta initially not matching transport filter
    seed := transport.TransportEntity{
        Type:    "Alpha",
        Value:   "a2",
        ChildRelations: []transport.TransportRelation{
            {Target: transport.TransportEntity{
                Type:    "Beta",
                Value:   "b2",
                ChildRelations: []transport.TransportRelation{
                    {Target: transport.TransportEntity{
                        Type:    "Gamma",
                        Value:   "g2",
                        ChildRelations: []transport.TransportRelation{
                            {Target: transport.TransportEntity{
                                Type:       "Delta",
                                Value:      "protoX",
                                Properties: map[string]string{"Transport": "plain"},
                            }},
                        },
                    }},
                },
            }},
        },
    }
    _ = mem.Mapper.MapTransportDataWithContext(seed, "Data")
    // Find Delta ID
    qDelta := gits.NewQuery().Read("Delta").Match("Value", "==", "protoX")
    rDelta := mem.Gits.Query().Execute(qDelta)
    if rDelta.Amount == 0 {
        t.Fatalf("expected seeded Delta to exist")
    }
    deltaID := rDelta.Entities[0].ID
    // Delta update: Transport -> secure (filter relevant). Should create 1 Job.
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: deltaID, Properties: map[string]string{"Transport": "secure"}}, "Data")
    sched.Run(mapped, cortex)
    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job after filter-relevant update, got %d", jobs.Amount)
    }
}

// Test 3 — Secondary-driven completion: add Delta under existing Alpha->Beta->Gamma
func Test_SecondaryDrivenCompletion_AddDelta_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)
    // Seed Alpha->Beta->Gamma (no Delta yet)
    mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "a3",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Beta", Value: "b3",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Gamma", Value: "g3"}}},
        }}},
    }, "Data")

    // Lookup Gamma ID
    qGamma := gits.NewQuery().Read("Gamma").Match("Value", "==", "g3")
    rGamma := mem.Gits.Query().Execute(qGamma)
    if rGamma.Amount == 0 {
        t.Fatalf("expected seeded Gamma to exist")
    }
    gammaID := rGamma.Entities[0].ID

    // Delta: attach a new Delta child with matching filters
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Gamma", ID: gammaID, ChildRelations: []transport.TransportRelation{{
        Target: transport.TransportEntity{Type: "Delta", ID: -1, Value: "protoX", Properties: map[string]string{"Transport": "secure"}},
    }}}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job after adding Delta to complete pattern, got %d", jobs.Amount)
    }
}

// Test 4 — Relation-only delta: pre-create Gamma and Delta, then just link them to complete the path
func Test_RelationOnlyDelta_CompletesEdge_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)
    // Seed Alpha->Beta->Gamma and separate Delta (unrelated)
    mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "a4",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Beta", Value: "b4",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Gamma", Value: "g4"}}},
        }}},
    }, "Data")

    mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", Value: "protoX", Properties: map[string]string{"Transport": "secure"}}, "Data")

    // Lookup IDs
    rG := mem.Gits.Query().Execute(gits.NewQuery().Read("Gamma").Match("Value", "==", "g4"))
    if rG.Amount == 0 {
        t.Fatalf("Gamma not found")
    }
    gammaID := rG.Entities[0].ID
    rD := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "protoX"))
    if rD.Amount == 0 {
        t.Fatalf("Delta not found")
    }
    deltaID := rD.Entities[0].ID

    // Delta: relation-only addition Gamma->Delta
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Gamma", ID: gammaID, ChildRelations: []transport.TransportRelation{{
        Target: transport.TransportEntity{Type: "Delta", ID: deltaID},
    }}}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job after relation-only edge creation, got %d", jobs.Amount)
    }
}

// Test 2.3 — Filter-relevant update on MATCH node (Value change to protoX)
// Seed Alpha->Beta->Gamma->Delta with Value!=protoX (e.g., protoY) and Transport=secure,
// then update Delta.Value to protoX -> expect 1 Job.
func Test_FilterRelevantUpdate_ValueChange_Triggers_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed full chain with non-matching Value on Delta (protoY), but Transport already secure
    seed := transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-val",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-val",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-val",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoY",
                    Properties: map[string]string{"Transport": "secure"},
                }}},
            }}},
        }}},
    }
    _ = mem.Mapper.MapTransportDataWithContext(seed, "Data")

    // Find Delta ID
    rDelta := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "protoY"))
    if rDelta.Amount == 0 {
        t.Fatalf("expected seeded Delta (protoY) to exist")
    }
    deltaID := rDelta.Entities[0].ID

    // Apply delta: change Value to protoX (filter-relevant equality)
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: deltaID, Value: "protoX"}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job after filter-relevant Value update, got %d", jobs.Amount)
    }
}

// Test 3.2 — Secondary-driven completion by attaching Alpha→Beta relation
// Seed Beta→Gamma→Delta(secure, protoX); delta: add Alpha→Beta relation → expect ActionB job.
func Test_AttachAlphaToBeta_CompletesDeepMatch_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed Beta->Gamma->Delta with matching filters on Delta
    seed := transport.TransportEntity{
        Type:  "Beta",
        Value: "b-attach",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Gamma",
            Value: "g-attach",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:       "Delta",
                Value:      "protoX",
                Properties: map[string]string{"Transport": "secure"},
            }}},
        }}},
    }
    _ = mem.Mapper.MapTransportDataWithContext(seed, "Data")

    // Lookup Beta ID to attach Alpha as parent via relation-only delta
    rB := mem.Gits.Query().Execute(gits.NewQuery().Read("Beta").Match("Value", "==", "b-attach"))
    if rB.Amount == 0 {
        t.Fatalf("seeded Beta not found")
    }
    betaID := rB.Entities[0].ID

    // Delta: create Alpha and link to existing Beta (relation-only; should be marked by bMap)
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "a-attach",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Beta", ID: betaID}}},
    }, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job after attaching Alpha→Beta, got %d", jobs.Amount)
    }
}

// Test 3.3 — Secondary-driven completion by inserting missing Beta between Alpha and Gamma
// Seed Alpha→Gamma→Delta(secure, protoX); delta: add Beta between Alpha and Gamma → expect ActionB job.
func Test_InsertBetaBetweenAlphaAndGamma_CompletesDeepMatch_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed Alpha -> Gamma -> Delta (filters satisfied at Delta), but missing Beta between Alpha and Gamma.
    seed := transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-insert",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Gamma",
            Value: "g-insert",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:       "Delta",
                Value:      "protoX",
                Properties: map[string]string{"Transport": "secure"},
            }}},
        }}},
    }
    _ = mem.Mapper.MapTransportDataWithContext(seed, "Data")

    // Lookup Alpha and Gamma IDs
    rA := mem.Gits.Query().Execute(gits.NewQuery().Read("Alpha").Match("Value", "==", "a-insert"))
    if rA.Amount == 0 { t.Fatalf("Alpha not found") }
    alphaID := rA.Entities[0].ID

    rG := mem.Gits.Query().Execute(gits.NewQuery().Read("Gamma").Match("Value", "==", "g-insert"))
    if rG.Amount == 0 { t.Fatalf("Gamma not found") }
    gammaID := rG.Entities[0].ID

    // Delta: add Beta between Alpha and Gamma by creating A→B and B→G relations in one payload.
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type: "Alpha", ID: alphaID,
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-insert",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Gamma", ID: gammaID}}},
        }}},
    }, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job after inserting Beta between Alpha and Gamma, got %d", jobs.Amount)
    }
}

// Test 4.1 — Relation-only delta Alpha→Beta that completes part of the dependency path
// Seed Alpha; and separately Beta→Gamma→Delta(secure, protoX). Delta: create relation Alpha→Beta.
func Test_RelationOnly_AlphaToBeta_CompletesPath_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed Alpha alone
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", Value: "a-rel41"}, "Data")

    // Seed Beta -> Gamma -> Delta (filters satisfied)
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type:  "Beta",
        Value: "b-rel41",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Gamma",
            Value: "g-rel41",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:       "Delta",
                Value:      "protoX",
                Properties: map[string]string{"Transport": "secure"},
            }}},
        }}},
    }, "Data")

    // Lookup Alpha and Beta IDs
    rA := mem.Gits.Query().Execute(gits.NewQuery().Read("Alpha").Match("Value", "==", "a-rel41"))
    if rA.Amount == 0 { t.Fatalf("Alpha not found") }
    alphaID := rA.Entities[0].ID

    rB := mem.Gits.Query().Execute(gits.NewQuery().Read("Beta").Match("Value", "==", "b-rel41"))
    if rB.Amount == 0 { t.Fatalf("Beta not found") }
    betaID := rB.Entities[0].ID

    // Delta: relation-only addition Alpha→Beta
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Alpha", ID: alphaID,
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Beta", ID: betaID}}},
    }, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job after relation-only Alpha→Beta, got %d", jobs.Amount)
    }
}

// Test 5.1 — Relation deletion should not schedule and breaks downstream matches
// Seed full satisfying path Alpha→Beta→Gamma→Delta(secure, protoX).
// Then delete relation Beta→Gamma. After that, even a filter-relevant update on Delta
// (e.g., Transport back to secure) must not schedule a job because the path is broken.
func Test_RelationDeletion_NoSchedule_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed full path
    seed := transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-del",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-del",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-del",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoX",
                    Properties: map[string]string{"Transport": "secure"},
                }}},
            }}},
        }}},
    }
    _ = mem.Mapper.MapTransportDataWithContext(seed, "Data")

    // Lookup Beta and Gamma IDs
    rB := mem.Gits.Query().Execute(gits.NewQuery().Read("Beta").Match("Value", "==", "b-del"))
    if rB.Amount == 0 { t.Fatalf("Beta not found") }
    betaID := rB.Entities[0].ID
    rG := mem.Gits.Query().Execute(gits.NewQuery().Read("Gamma").Match("Value", "==", "g-del"))
    if rG.Amount == 0 { t.Fatalf("Gamma not found") }
    gammaID := rG.Entities[0].ID

    // Delete relation Beta→Gamma via Unlink
    unlink := query.New().Unlink("Beta").Match("ID", "==", strconv.Itoa(betaID)).To(
        query.New().Find("Gamma").Match("ID", "==", strconv.Itoa(gammaID)),
    )
    _ = mem.Gits.Query().Execute(unlink)

    // Find Delta ID and toggle transport away from secure (no job expected)
    rD := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "protoX"))
    if rD.Amount == 0 { t.Fatalf("Delta not found") }
    deltaID := rD.Entities[0].ID
    mapped1 := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: deltaID, Properties: map[string]string{"Transport": "plain"}}, "Data")
    sched.Run(mapped1, cortex)
    jobs0 := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs0.Amount != 0 {
        t.Fatalf("expected 0 jobs after breaking path and setting Transport=plain, got %d", jobs0.Amount)
    }

    // Now set it back to secure (filter-relevant) — still no job due to missing Beta→Gamma
    mapped2 := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: deltaID, Properties: map[string]string{"Transport": "secure"}}, "Data")
    sched.Run(mapped2, cortex)
    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 0 {
        t.Fatalf("expected 0 jobs after relation deletion even with relevant update, got %d", jobs.Amount)
    }
}

// Test 5.2 — Re-creating the deleted relation should schedule once
// Continue from the same seeded structure idea: after deleting Beta→Gamma (as in 5.1),
// re-create the relation via a relation-only delta and expect exactly one job.
func Test_RecreateRelation_SchedulesOnce_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed full path
    seed := transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-readd",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-readd",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-readd",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoX",
                    Properties: map[string]string{"Transport": "secure"},
                }}},
            }}},
        }}},
    }
    _ = mem.Mapper.MapTransportDataWithContext(seed, "Data")

    // Lookup Beta and Gamma IDs and delete the relation
    rB := mem.Gits.Query().Execute(gits.NewQuery().Read("Beta").Match("Value", "==", "b-readd"))
    if rB.Amount == 0 { t.Fatalf("Beta not found") }
    betaID := rB.Entities[0].ID
    rG := mem.Gits.Query().Execute(gits.NewQuery().Read("Gamma").Match("Value", "==", "g-readd"))
    if rG.Amount == 0 { t.Fatalf("Gamma not found") }
    gammaID := rG.Entities[0].ID
    unlink := query.New().Unlink("Beta").Match("ID", "==", strconv.Itoa(betaID)).To(
        query.New().Find("Gamma").Match("ID", "==", strconv.Itoa(gammaID)),
    )
    _ = mem.Gits.Query().Execute(unlink)

    // Re-create relation via relation-only delta (should be marked by bMap on relation)
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Beta", ID: betaID, ChildRelations: []transport.TransportRelation{{
        Target: transport.TransportEntity{Type: "Gamma", ID: gammaID},
    }}}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job after re-creating Beta→Gamma, got %d", jobs.Amount)
    }
}

// Test 4.3 — Relation properties/context update should not schedule (no relation-level filters modeled)
// Seed full satisfying path; delta updates only relation properties on Gamma→Delta; expect 0 jobs.
func Test_RelationPropertyUpdate_NoSchedule_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed Alpha -> Beta -> Gamma -> Delta with filters satisfied on Delta
    seed := transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-relprops",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-relprops",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-relprops",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoX",
                    Properties: map[string]string{"Transport": "secure"},
                }}},
            }}},
        }}},
    }
    _ = mem.Mapper.MapTransportDataWithContext(seed, "Data")

    // Lookup Gamma and Delta IDs
    rG := mem.Gits.Query().Execute(gits.NewQuery().Read("Gamma").Match("Value", "==", "g-relprops"))
    if rG.Amount == 0 { t.Fatalf("Gamma not found") }
    gammaID := rG.Entities[0].ID
    rD := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "protoX"))
    if rD.Amount == 0 { t.Fatalf("Delta not found") }
    deltaID := rD.Entities[0].ID

    // Delta: Update only relation properties on Gamma→Delta. Mapper won't mark bMap on relation updates.
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Gamma", ID: gammaID, ChildRelations: []transport.TransportRelation{{
        // Set some relation properties/context; scheduler should not treat this as trigger
        Properties: map[string]string{"EdgeAttr": "tweak"},
        Target:     transport.TransportEntity{Type: "Delta", ID: deltaID},
    }}}, "Data")

    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 0 {
        t.Fatalf("expected 0 jobs after relation property/context update, got %d", jobs.Amount)
    }
}

// Test 6.3 — Non-relevant property change on a Secondary node (Gamma) should not schedule
func Test_NonRelevantSecondaryUpdate_NoSchedule_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed Alpha -> Beta -> Gamma -> Delta with filters satisfied on Delta
    seed := transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-secupd",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-secupd",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-secupd",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoX",
                    Properties: map[string]string{"Transport": "secure"},
                }}},
            }}},
        }}},
    }
    _ = mem.Mapper.MapTransportDataWithContext(seed, "Data")

    // Lookup Gamma ID
    rG := mem.Gits.Query().Execute(gits.NewQuery().Read("Gamma").Match("Value", "==", "g-secupd"))
    if rG.Amount == 0 { t.Fatalf("Gamma not found") }
    gammaID := rG.Entities[0].ID

    // Delta: non-relevant property change on Gamma (no filters on Gamma in ActionB)
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Gamma", ID: gammaID, Properties: map[string]string{"Tag": "foo"}}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 0 {
        t.Fatalf("expected 0 jobs after non-relevant Gamma property update, got %d", jobs.Amount)
    }
}

// Test 6.1 — Seed Alpha→Beta→Gamma→Delta(Value!=protoX, Transport=secure);
// delta: update Delta.Value→protoX → expect ActionB job.
func Test_SecondaryUpdate_ValueToProtoX_Schedules_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed full chain with Delta.Value != protoX (e.g., protoY), Transport already secure
    seed := transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-61",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-61",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-61",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoY",
                    Properties: map[string]string{"Transport": "secure"},
                }}},
            }}},
        }}},
    }
    _ = mem.Mapper.MapTransportDataWithContext(seed, "Data")

    // Find Delta (protoY)
    rD := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "protoY"))
    if rD.Amount == 0 { t.Fatalf("Delta (protoY) not found") }
    deltaID := rD.Entities[0].ID

    // Delta: change Value to protoX (filter relevant)
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: deltaID, Value: "protoX"}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job for 6.1 value update, got %d", jobs.Amount)
    }
}

// Test 6.2 — Seed Alpha→Beta→Gamma→Delta(Value=protoX, Transport!=secure);
// delta: update Delta.Transport→secure → expect ActionB job.
func Test_SecondaryUpdate_TransportToSecure_Schedules_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed full chain with Delta.Value=protoX but Transport not secure (plain)
    seed := transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-62",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-62",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-62",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoX",
                    Properties: map[string]string{"Transport": "plain"},
                }}},
            }}},
        }}},
    }
    _ = mem.Mapper.MapTransportDataWithContext(seed, "Data")

    // Find Delta (protoX)
    rD := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "protoX"))
    if rD.Amount == 0 { t.Fatalf("Delta (protoX) not found") }
    // If multiple, pick the one under this seed by relation to Gamma; for simplicity, take first as tests run isolated
    deltaID := rD.Entities[0].ID

    // Delta: change Transport to secure (filter relevant)
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: deltaID, Properties: map[string]string{"Transport": "secure"}}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job for 6.2 transport update, got %d", jobs.Amount)
    }
}

// Test 10.1 — Enrichment from existing Gits state (left side seeded, right side via delta)
// Seed Alpha→Beta on the left side. Separately seed Gamma (no Delta yet). Then, in a single
// delta on the right side, attach Delta (secure, protoX) under Gamma and also link Gamma to the
// existing Beta via a parent relation. Expect one job for ActionB through enrichment that joins
// the left and right fragments.
func Test_Enrichment_LeftAlphaBeta_RightGammaDelta_DeltaCompletesPath_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed left fragment: Alpha -> Beta
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-101",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-101",
        }}},
    }, "Data")

    // Seed right anchor node only: Gamma (no parents/children yet)
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Gamma", Value: "g-101"}, "Data")

    // Lookup Beta and Gamma IDs to connect in the delta
    rB := mem.Gits.Query().Execute(gits.NewQuery().Read("Beta").Match("Value", "==", "b-101"))
    if rB.Amount == 0 { t.Fatalf("Beta not found") }
    betaID := rB.Entities[0].ID
    rG := mem.Gits.Query().Execute(gits.NewQuery().Read("Gamma").Match("Value", "==", "g-101"))
    if rG.Amount == 0 { t.Fatalf("Gamma not found") }
    gammaID := rG.Entities[0].ID

    // Delta on the right side only:
    //  - create Delta under Gamma with filters satisfied
    //  - link Gamma back to existing Beta via ParentRelations
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type: "Gamma", ID: gammaID,
        ParentRelations: []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Beta", ID: betaID}}},
        ChildRelations:  []transport.TransportRelation{{Target: transport.TransportEntity{Type: "Delta", Value: "protoX", Properties: map[string]string{"Transport": "secure"}}}},
    }, "Data")

    // Run scheduler: enrichment should now see Alpha->Beta->Gamma->Delta
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job for enrichment across left/right fragments, got %d", jobs.Amount)
    }
}

// Test 10.2 — Enrichment with delta-only input (no children in payload)
// Seed full chain where Delta does not initially match filters. Then apply a delta that only updates
// the Delta node (no children/relations in payload). The scheduler should enrich the rest from storage
// and schedule one job when filters become satisfied.
func Test_Enrichment_DeltaOnlyUpdate_EnrichFindsPartners_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed full chain with Delta not matching (Transport=plain)
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-102",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-102",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-102",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoX",
                    Properties: map[string]string{"Transport": "plain"},
                }}},
            }}},
        }}},
    }, "Data")

    // Lookup Delta ID
    rD := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "protoX"))
    if rD.Amount == 0 { t.Fatalf("Delta not found") }
    deltaID := rD.Entities[0].ID

    // Delta-only update: change Transport to secure (no children or relations in payload)
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: deltaID, Properties: map[string]string{"Transport": "secure"}}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job after delta-only update with enrichment, got %d", jobs.Amount)
    }
}

// Test 12.1 — Strict causality: construct a match via enrichment that does NOT include
// any changed entity from this batch → ensure no job is scheduled.
// Strategy:
//  - Seed a complete matching path for ActionB: Alpha→Beta→Gamma→Delta(D1 matches filters)
//  - Create another, unrelated Delta entity D2
//  - Delta: update D2 (unrelated). Scheduler can still enrich and find the full path via D1,
//    but inputContainsUpdated must fail because D2 is not part of the constructed input.
func Test_StrictCausality_EnrichmentWithoutDeltaInInput_NoJob_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed complete matching path with D1
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-121",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-121",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-121",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoX",
                    Properties: map[string]string{"Transport": "secure"},
                }}},
            }}},
        }}},
    }, "Data")

    // Create an unrelated Delta D2
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", Value: "protoY", Properties: map[string]string{"Transport": "plain"}}, "Data")

    // Find D2 ID
    rD2 := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "protoY"))
    if rD2.Amount == 0 { t.Fatalf("unrelated Delta D2 not found") }
    d2ID := rD2.Entities[0].ID

    // Delta: update D2 (unrelated). Even if enrichment can find the full path using D1,
    // strict causality should prevent scheduling because the delta entity (D2) is not in the input.
    mapped := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: d2ID, Properties: map[string]string{"Tag": "noop"}}, "Data")
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 0 {
        t.Fatalf("expected 0 jobs when match is via enrichment but delta entity is not part of input, got %d", jobs.Amount)
    }
}

// Test 13.1 — Sequential idempotency (policy A: output-as-dedupe / no-op delta):
// Re-apply the same delta twice sequentially; the second application should not create
// an additional job because it results in no change (mapper won't mark bMap).
func Test_Idempotency_ReapplySameDelta_NoSecondJob_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed chain where Delta does NOT match initially (Transport=plain)
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-131",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-131",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-131",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoX",
                    Properties: map[string]string{"Transport": "plain"},
                }}},
            }}},
        }}},
    }, "Data")

    // Lookup Delta ID
    rD := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "protoX"))
    if rD.Amount == 0 { t.Fatalf("Delta not found") }
    dID := rD.Entities[0].ID

    // First delta: change Transport to secure (relevant) → expect one job
    mapped1 := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: dID, Properties: map[string]string{"Transport": "secure"}}, "Data")
    sched.Run(mapped1, cortex)
    jobs1 := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs1.Amount != 1 { t.Fatalf("expected 1 job after first relevant update, got %d", jobs1.Amount) }

    // Second delta: re-apply the same update (no actual change) → expect still one job total
    mapped2 := mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: dID, Properties: map[string]string{"Transport": "secure"}}, "Data")
    sched.Run(mapped2, cortex)
    jobs2 := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs2.Amount != 1 {
        t.Fatalf("expected still 1 job after re-applying identical delta, got %d", jobs2.Amount)
    }
}

// Test 13.2 — Parallel idempotency: run the same relevant delta in two goroutines
// against the same instance; assert a single job is created overall.
func Test_Idempotency_ParallelSameDelta_SingleJob_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed full chain with Delta not matching (Transport=plain)
    _ = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-132",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-132",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-132",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoX",
                    Properties: map[string]string{"Transport": "plain"},
                }}},
            }}},
        }}},
    }, "Data")

    // Lookup Delta ID
    rD := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "protoX"))
    if rD.Amount == 0 { t.Fatalf("Delta not found") }
    dID := rD.Entities[0].ID

    // Prepare the same delta mapping in two goroutines
    var mapped1, mapped2 transport.TransportEntity
    var wg sync.WaitGroup
    wg.Add(2)
    go func() {
        defer wg.Done()
        mapped1 = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: dID, Properties: map[string]string{"Transport": "secure"}}, "Data")
    }()
    go func() {
        defer wg.Done()
        mapped2 = mem.Mapper.MapTransportDataWithContext(transport.TransportEntity{Type: "Delta", ID: dID, Properties: map[string]string{"Transport": "secure"}}, "Data")
    }()
    wg.Wait()

    // Run scheduler for both mapped deltas
    sched.Run(mapped1, cortex)
    sched.Run(mapped2, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected exactly 1 job after parallel identical deltas, got %d", jobs.Amount)
    }
}

// Test 14.1 — Context handling: Entities/relations in different Contexts still satisfy
// dependencies under current policy (no context constraints in scheduler queries).
func Test_Context_CrossContextMatch_Allowed_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed left-side path Alpha->Beta->Gamma in Context "Left" using MapTransportData (no overwrite)
    _ = mem.Mapper.MapTransportData(transport.TransportEntity{
        Type:    "Alpha",
        Value:   "a-141",
        Context: "Left",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:    "Beta",
            Value:   "b-141",
            Context: "Left",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:    "Gamma",
                Value:   "g-141",
                Context: "Left",
            }}},
        }}},
    })

    // Create Delta in Context "Right" and link Gamma->Delta via a delta mapped without overwriting context
    // First, find Gamma ID to attach child
    rG := mem.Gits.Query().Execute(gits.NewQuery().Read("Gamma").Match("Value", "==", "g-141"))
    if rG.Amount == 0 { t.Fatalf("Gamma not found in Left context") }
    gammaID := rG.Entities[0].ID

    mapped := mem.Mapper.MapTransportData(transport.TransportEntity{
        Type:    "Gamma",
        ID:      gammaID,
        Context: "Left",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:       "Delta",
            Value:      "protoX",
            Context:    "Right",
            Properties: map[string]string{"Transport": "secure"},
        }}},
    })

    // Run scheduler and expect one job since cross-context paths are considered
    sched.Run(mapped, cortex)
    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 1 {
        t.Fatalf("expected 1 job for cross-context match under current policy, got %d", jobs.Amount)
    }
}

// Test 14.2 — Changing Context on a filter-relevant node (Delta) should not schedule
// under current policy because context is not part of filters and Mapper does not
// treat Context changes as property updates (no bMap). We document this behavior.
func Test_Context_ChangeOnMatchNode_NoSchedule_ActionB(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionB}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)

    // Seed full path where Delta already matches filters in Context "Left"
    seed := transport.TransportEntity{
        Type:  "Alpha",
        Value: "a-142",
        ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
            Type:  "Beta",
            Value: "b-142",
            ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                Type:  "Gamma",
                Value: "g-142",
                ChildRelations: []transport.TransportRelation{{Target: transport.TransportEntity{
                    Type:       "Delta",
                    Value:      "protoX",
                    Context:    "Left",
                    Properties: map[string]string{"Transport": "secure"},
                }}},
            }}},
        }}},
    }
    _ = mem.Mapper.MapTransportDataWithContext(seed, "Data")

    // Lookup Delta ID (Context Left)
    rD := mem.Gits.Query().Execute(gits.NewQuery().Read("Delta").Match("Value", "==", "protoX"))
    if rD.Amount == 0 { t.Fatalf("Delta not found for context change test") }
    deltaID := rD.Entities[0].ID

    // Delta: attempt to change only the Context to "Right".
    // Note: handleExistingEntityProperties doesn't track Context changes as updates (no bMap),
    // so scheduler should not schedule due to lack of causality markers or relation changes.
    mapped := mem.Mapper.MapTransportData(transport.TransportEntity{Type: "Delta", ID: deltaID, Context: "Right"})
    sched.Run(mapped, cortex)

    jobs := mem.Gits.Query().Execute(gits.NewQuery().Read("Job"))
    if jobs.Amount != 0 {
        t.Fatalf("expected 0 jobs after context-only change on MATCH node, got %d", jobs.Amount)
    }
}
