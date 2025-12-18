package scheduler

import (
    "testing"

    "github.com/voodooEntity/gits/src/transport"
    cerebrum "github.com/voodooEntity/cyberbrain/src/system/cerebrum"
    cfgb "github.com/voodooEntity/cyberbrain/src/system/configBuilder"
    "github.com/voodooEntity/cyberbrain/src/system/interfaces"
)

// actionAlias — dependency with two same-type children using explicit aliases
type actionAlias struct{}

func (a *actionAlias) Execute(input transport.TransportEntity, requirement, context, jobID string) ([]transport.TransportEntity, error) {
    return nil, nil
}

func (a *actionAlias) GetConfig() transport.TransportEntity {
    cfg := cfgb.NewConfig().SetName("ActionAliasSiblings").SetCategory("Test")
    root := cfgb.NewStructure("Root").SetPriority(cfgb.PRIORITY_PRIMARY).SetMode(cfgb.MODE_SET)
    // Two children of same type with explicit aliases
    left := cfgb.NewStructure("Beta").SetAlias("left").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET)
    right := cfgb.NewStructure("Beta").SetAlias("right").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_SET)
    root = root.AddChild(left).AddChild(right)
    cfg.AddDependency("siblings", root)
    return cfg.Build()
}

func newActionAlias() interfaces.ActionInterface { return &actionAlias{} }

// actionNorm — dependency with Properties.Transport filter to test normalization
type actionNorm struct{}

func (a *actionNorm) Execute(input transport.TransportEntity, requirement, context, jobID string) ([]transport.TransportEntity, error) {
    return nil, nil
}

func (a *actionNorm) GetConfig() transport.TransportEntity {
    cfg := cfgb.NewConfig().SetName("ActionFilterNorm").SetCategory("Test")
    root := cfgb.NewStructure("Alpha").SetPriority(cfgb.PRIORITY_PRIMARY).SetMode(cfgb.MODE_SET)
    // Add Delta with a Properties.Transport MATCH filter
    delta := cfgb.NewStructure("Delta").SetPriority(cfgb.PRIORITY_SECONDARY).SetMode(cfgb.MODE_MATCH).
        AddFilter("Transport", "Properties.Transport", "==", "secure")
    root = root.AddChild(delta)
    cfg.AddDependency("alphaDelta", root)
    return cfg.Build()
}

func newActionNorm() interfaces.ActionInterface { return &actionNorm{} }

// Test: compiled pattern should order children by alias (left before right)
func Test_PatternCompile_AliasSiblingOrdering(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionAlias}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)
    _ = mem // not used further; kept for symmetry

    pn := sched.DebugGetCompiledPattern(cortex, "ActionAliasSiblings", "siblings")
    if pn == nil {
        t.Fatalf("compiled pattern not found")
    }
    if len(pn.Children) != 2 {
        t.Fatalf("expected 2 children under root, got %d", len(pn.Children))
    }
    if pn.Children[0].Alias != "left" || pn.Children[1].Alias != "right" {
        t.Fatalf("expected alias order [left,right], got [%s,%s]", pn.Children[0].Alias, pn.Children[1].Alias)
    }
    if pn.Children[0].Type != "Beta" || pn.Children[1].Type != "Beta" {
        t.Fatalf("expected both children Type=Beta, got %s and %s", pn.Children[0].Type, pn.Children[1].Type)
    }
}

// Test: filter normalization should include key Transport for Properties.Transport filter
func Test_PatternCompile_FilterNormalization(t *testing.T) {
    actions := []func() interfaces.ActionInterface{newActionNorm}
    sched, mem, cortex := setupFreshAndSeed(nil, actions)
    _ = mem

    pn := sched.DebugGetCompiledPattern(cortex, "ActionFilterNorm", "alphaDelta")
    if pn == nil {
        t.Fatalf("compiled pattern not found")
    }
    // find Delta node
    var findDelta func(n *cerebrum.PatternNode) *cerebrum.PatternNode
    findDelta = func(n *cerebrum.PatternNode) *cerebrum.PatternNode {
        if n == nil { return nil }
        if n.Type == "Delta" { return n }
        for _, ch := range n.Children {
            if d := findDelta(ch); d != nil { return d }
        }
        return nil
    }
    dn := findDelta(pn)
    if dn == nil {
        t.Fatalf("Delta node not found in compiled pattern")
    }
    if _, ok := dn.NormalizedFilterFields["Transport"]; !ok {
        t.Fatalf("expected normalized filter field 'Transport' in Delta node, got %+v", dn.NormalizedFilterFields)
    }
}
