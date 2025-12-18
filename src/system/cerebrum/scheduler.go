package cerebrum

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/voodooEntity/gits/src/query"
	"github.com/voodooEntity/gits/src/transport"
	"github.com/voodooEntity/cyberbrain/src/system/archivist"
	"github.com/voodooEntity/cyberbrain/src/system/util"
)

type Scheduler struct {
	memory        *Memory
	demultiplexer *Demultiplexer
	log           *archivist.Archivist
	// compiled dependency patterns cache: key = action|depID
	patternCache map[string]*PatternNode
	// cache diagnostics
	patternHits   int
	patternMisses int
	// track if we already printed a compile summary per key
	patternSummarized map[string]bool
}

func NewScheduler(memory *Memory, demultiplexerInstance *Demultiplexer, logger *archivist.Archivist) *Scheduler {
	return &Scheduler{
		memory:            memory,
		demultiplexer:     demultiplexerInstance,
		log:               logger,
		patternCache:      make(map[string]*PatternNode),
		patternSummarized: make(map[string]bool),
	}
}

func (s *Scheduler) Run(data transport.TransportEntity, cortex *Cortex) {
	// scheduling: acknowledge that returned job output may be a subgraph; enrichment can extend upwards
	s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED RUN begin root=", data.Type, ":", data.ID)
	// We first identify potentially relevant actions/dependencies for this input batch.
	// discover relation structures present in this batch (for relation-only triggers)
	newRelationStructures := make(map[string][2]*transport.TransportEntity)
	newRelationStructures = s.rFilterRelationStructures(data, newRelationStructures)

	// build a lightweight lookup from the raw batch (without demux) to find candidate actions
	lookup := make(map[string]int)
	var pointer [][]*transport.TransportEntity
	lookup, pointer = s.rEnrichLookupAndPointer(data, lookup, pointer)

	var actionsAndDependencies [][2]string
	for entityType := range lookup {
		actionsAndDependencies = append(actionsAndDependencies, s.retrieveActionsByType(entityType)...)
	}
	if 0 < len(newRelationStructures) {
		actionsAndDependencies = s.enrichActionsAndDependenciesByNewRelationStructures(newRelationStructures, actionsAndDependencies)
	}

	// collect allowed child types for the input root from the dependencies we found
	allowed := map[string]bool{}
	for _, ad := range actionsAndDependencies {
		act, _ := cortex.GetAction(ad[0])
		req := act.GetDependencyByName(ad[1])
		node := s.findNodeByValue(req.Children()[0], data.Type)
		if node != nil {
			for _, ch := range node.Children() {
				allowed[ch.Value] = true
			}
		}
	}
	// Log candidates compactly
	if len(actionsAndDependencies) > 0 {
		var cand []string
		for _, ad := range actionsAndDependencies {
			cand = append(cand, ad[0]+":"+ad[1])
		}
		s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED candidates=", cand)
	} else {
		s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED candidates=[]")
	}

	// Filter input children to only allowed types (if any), then demultiplex
	filtered := data
	if len(allowed) > 0 && len(data.ChildRelations) > 0 {
		var kept []transport.TransportRelation
		for _, cr := range data.ChildRelations {
			if allowed[cr.Target.Type] {
				kept = append(kept, cr)
			}
		}
		filtered.ChildRelations = kept
	}

	// extract batch anchors (entities with bMap and relation-only child endpoints).
	anchors := s.extractBatchAnchors(data, newRelationStructures)
	if len(anchors) > 0 {
		s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED anchors=", len(anchors))
	} else {
		s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED anchors=0")
	}

	// overlay-only path: if no anchors were detected, treat the batch root as a single anchor.
	if len(anchors) == 0 {
		anchors = append(anchors, data)
	}
	s.overlayProcessAnchors(anchors, actionsAndDependencies, data, newRelationStructures, cortex)
}

// findNodeByValue searches a dependency tree for a node whose Value matches the given type name.
func (s *Scheduler) findNodeByValue(root transport.TransportEntity, typeName string) *transport.TransportEntity {
	if root.Value == typeName {
		return &root
	}
	for _, ch := range root.Children() {
		if hit := s.findNodeByValue(ch, typeName); hit != nil {
			return hit
		}
	}
	return nil
}

// extractBatchAnchors collects anchor entities for this batch.
// - Any entity in the input graph with a bMap property is considered an entity anchor.
// - For relation-only deltas detected in newRelationStructures, we consider only the child endpoint as an anchor.
func (s *Scheduler) extractBatchAnchors(entity transport.TransportEntity, relationStructures map[string][2]*transport.TransportEntity) []transport.TransportEntity {
	anchors := make([]transport.TransportEntity, 0, 4)
	// walk the input graph to find entities marked by bMap
	var walk func(e transport.TransportEntity)
	walk = func(e transport.TransportEntity) {
		if e.Properties != nil {
			if _, ok := e.Properties["bMap"]; ok {
				anchors = append(anchors, e)
			}
		}
		for _, cr := range e.ChildRelations {
			walk(cr.Target)
		}
		for _, pr := range e.ParentRelations {
			walk(pr.Target)
		}
	}
	walk(entity)

	// add child endpoints from relation-only structures
	for _, pair := range relationStructures {
		// pair[1] is the child target in rFilterRelationStructures
		if pair[1] != nil {
			anchors = append(anchors, *pair[1])
		}
	}
	return anchors
}

// overlayProcessAnchors implements a minimal anchor-driven overlay:
// for each anchor, it restricts candidates to actions whose pattern contains the
// anchor type, builds lookup/pointer from the anchor subgraph, constructs inputs,
// then enforces causality and idempotency before creating jobs.
func (s *Scheduler) overlayProcessAnchors(anchors []transport.TransportEntity, actionsAndDependencies [][2]string, batch transport.TransportEntity, newRelationStructures map[string][2]*transport.TransportEntity, cortex *Cortex) {
	// Pre-compute updated entity IDs from the full batch to enforce strict causality.
	updatedIDs := s.collectUpdatedEntityIDs(batch, newRelationStructures)
	// Collect bMap updated keys at batch root (common case: single-entity updates)
	batchBMapValue := ""
	if batch.Properties != nil {
		if v, ok := batch.Properties["bMap"]; ok {
			batchBMapValue = v
		}
	}

	for _, anchor := range anchors {
		// Build a tiny lookup/pointer starting only from the anchor entity.
		lookup := make(map[string]int)
		var pointer [][]*transport.TransportEntity
		lookup, pointer = s.rEnrichLookupAndPointer(anchor, lookup, pointer)

		for _, ad := range actionsAndDependencies {
			act, _ := cortex.GetAction(ad[0])
			requirement := act.GetDependencyByName(ad[1])
			// Ensure the compiled pattern for this dependency contains the anchor type.
			if !s.patternContainsType(act.GetName(), requirement, anchor.Type) {
				continue
			}
			// If this batch carries property updates (bMap with keys) and the dependency is MATCH-mode
			// on fields that are not among updated keys, skip due to irrelevance.
			if batchBMapValue != "" {
				if !s.hasRelevantFilter(requirement, batchBMapValue) {
					s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED RELEVANCE matchedKey=none (skip)")
					continue
				}
			}
			// Build candidate inputs using existing query builder, constrained by lookup.
			inputs := s.buildInputData(requirement.Children()[0], lookup, pointer)
			for _, input := range inputs {
				// Ensure the constructed input contains the anchor entity (Type,ID).
				if !s.inputContainsEntity(&input, anchor.Type, anchor.ID) {
					continue
				}
				// Enforce strict causality: input must include an updated entity from this batch.
				if !s.inputContainsUpdated(&input, updatedIDs) {
					s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED CAUSALITY action=", act.GetName(), " dep=", ad[1], " containsUpdated=", false)
					continue
				}
				s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED CAUSALITY action=", act.GetName(), " dep=", ad[1], " containsUpdated=", true)
				sig := util.GenerateSignature(input)
				// Witness / Memory idempotency guard
				if s.isDuplicateByWitness(act.GetName(), ad[1], input, requirement) {
					s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED JOB skip duplicate by Memory witness action=", act.GetName(), " dep=", ad[1])
					continue
				}
				s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED JOB create action=", act.GetName(), " dep=", ad[1], " sig=", sig)
				newJob := NewJob(s.memory, s.log)
				created := newJob.Create(act.GetName(), ad[1], input)
				s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED JOB persisted id=", created.id, " action=", act.GetName(), " dep=", ad[1])
			}
		}
	}
}

// patternContainsType returns true if the compiled pattern for the dependency contains
// a node with the given type.
func (s *Scheduler) patternContainsType(actionName string, dep transport.TransportEntity, typeName string) bool {
	pn := s.getOrCompilePattern(actionName, dep)
	var has func(n *PatternNode) bool
	has = func(n *PatternNode) bool {
		if n == nil {
			return false
		}
		if n.Type == typeName {
			return true
		}
		for _, ch := range n.Children {
			if has(ch) {
				return true
			}
		}
		return false
	}
	return has(pn)
}

// inputContainsEntity checks whether the input graph includes (Type,ID).
func (s *Scheduler) inputContainsEntity(input *transport.TransportEntity, t string, id int) bool {
	found := false
	s.rWalkInput(input, func(e *transport.TransportEntity) {
		if !found && e.Type == t && e.ID == id {
			found = true
		}
	})
	return found
}

func (s *Scheduler) rFilterRelationStructures(entity transport.TransportEntity, relationStructures map[string][2]*transport.TransportEntity) map[string][2]*transport.TransportEntity {
	if 0 < len(entity.ChildRelations) {
		for _, childRelation := range entity.ChildRelations {
			if _, ok := childRelation.Properties["bMap"]; ok && childRelation.Properties["bMap"] == "" { // #
				tmpRelString := entity.Type + "-" + childRelation.Target.Type
				add := true
				for knownRelString, _ := range relationStructures {
					if knownRelString == tmpRelString {
						add = false
					}
				}
				if add {
					relationStructures[tmpRelString] = [2]*transport.TransportEntity{&entity, &childRelation.Target}
				}
			}
			relationStructures = s.rFilterRelationStructures(childRelation.Target, relationStructures)
		}
	}
	return relationStructures
}

func (s *Scheduler) createNewJobs(entity transport.TransportEntity, newRelationStructures map[string][2]*transport.TransportEntity, cortex *Cortex) []transport.TransportEntity {
	// first we will enrich some lookup variables we need later on
	// by recursively walking the given data
	lookup := make(map[string]int)
	var pointer [][]*transport.TransportEntity
	s.log.DebugF(archivist.DEBUG_LEVEL_MAX, "Enrich lookup by entity %+v", entity)
	lookup, pointer = s.rEnrichLookupAndPointer(entity, lookup, pointer)
	s.log.Debug(archivist.DEBUG_LEVEL_MAX, "Lookup data", lookup, pointer)
	// now we going to retrieve all action+dependency combos to that could potentially
	// be executed based on the new learned data which we just identified and stored
	// in our lookup/pointer variables
	var actionsAndDependencies [][2]string
	for entityType := range lookup {
		actionsAndDependencies = append(actionsAndDependencies, s.retrieveActionsByType(entityType)...)
	}
	s.log.Debug(archivist.DEBUG_LEVEL_MAX, "Action and dependency found to input", actionsAndDependencies)

	// now also gonne lookup & enrich the actionsAndDependencies based on the newRelationStructures
	if 0 < len(newRelationStructures) {
		s.log.Debug(archivist.DEBUG_LEVEL_MAX, "New relevant relation structures found in scheduler %+v", newRelationStructures)
		s.log.Debug(archivist.DEBUG_LEVEL_MAX, "actionsAndDependencies before enrichin by relation structures", actionsAndDependencies)
		actionsAndDependencies = s.enrichActionsAndDependenciesByNewRelationStructures(newRelationStructures, actionsAndDependencies)
		s.log.Debug(archivist.DEBUG_LEVEL_MAX, "actionsAndDependencies after enrichin by relation structures", actionsAndDependencies)
		s.log.Debug(archivist.DEBUG_LEVEL_MAX, "lookupAndPointer before enrichment by relation structures", lookup, pointer)
		lookup, pointer = s.enrichLookupAndPointerByRelationStructures(newRelationStructures, lookup, pointer)
		s.log.Debug(archivist.DEBUG_LEVEL_MAX, "lookupAndPointer after enrichment by relation structures", lookup, pointer)
		s.log.Debug(archivist.DEBUG_LEVEL_MAX, "lookupAndPointer input structure", entity)
	}

	// at this point we go a single possible input structure and all potential actions/dependencies
	// that could be satisfied using it. Now we're going to try build actual input data by walking
	// through the dependencies and enrich an input datastructure using the given entity data and
	// the data that is in our storage
	// Pre-compute the set of updated entity IDs from this batch to enforce strict delta causality
	updatedIDs := s.collectUpdatedEntityIDs(entity, newRelationStructures)
	for _, actionAndDependency := range actionsAndDependencies {
		act, _ := cortex.GetAction(actionAndDependency[0])
		requirement := act.GetDependencyByName(actionAndDependency[1])

		//  compile and cache dependency pattern (read-only). not used in construction yet.
		_ = s.getOrCompilePattern(act.GetName(), requirement)

		bMapValue, bMapExists := entity.Properties["bMap"]
		// Skip if bMap exists and has a value, but none of the updated properties match a filter in the requirement
		if bMapExists && bMapValue != "" {
			if !s.hasRelevantFilter(requirement, bMapValue) {
				s.log.DebugF(archivist.DEBUG_LEVEL_DUMP, "Skipping job creation as updated properties are not relevant for action %+v", actionAndDependency[0])
				continue
			}
		}

		s.log.DebugF(archivist.DEBUG_LEVEL_DUMP, "Trying to enrich data based on %+v ", actionAndDependency)
		newJobInputs := s.buildInputData(requirement.Children()[0], lookup, pointer)
		//inputData, err := rBuildInputData(requirement.Children()[0], entity, pointer, lookup, false, "", -1, nil)
		if 0 < len(newJobInputs) {
			for _, inputData := range newJobInputs {
				sig := util.GenerateSignature(inputData)
				s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED INPUT signature=", sig)
				s.log.DebugF(archivist.DEBUG_LEVEL_DUMP, "Created a new job with payload %+v", inputData)
				// Strict delta causality: only schedule if the input contains at least one of the updated entities/relations
				if !s.inputContainsUpdated(&inputData, updatedIDs) {
					s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED CAUSALITY action=", act.GetName(), " dep=", actionAndDependency[1], " containsUpdated=", false)
					continue
				}
				s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED CAUSALITY action=", act.GetName(), " dep=", actionAndDependency[1], " containsUpdated=", true)
				s.log.DebugF(archivist.DEBUG_LEVEL_DUMP, "Created a new job with payload %+v", inputData)
				// Witness / Memory idempotency guard (anchor-sharded, no global index)
				if s.isDuplicateByWitness(act.GetName(), actionAndDependency[1], inputData, requirement) {
					s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED JOB skip duplicate by Memory witness action=", act.GetName(), " dep=", actionAndDependency[1])
					continue
				}
				s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED JOB create action=", act.GetName(), " dep=", actionAndDependency[1], " sig=", sig)
				newJob := NewJob(s.memory, s.log)
				created := newJob.Create(act.GetName(), actionAndDependency[1], inputData)
				s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED JOB persisted id=", created.id, " action=", act.GetName(), " dep=", actionAndDependency[1])
			}
		} else {
			s.log.DebugF(archivist.DEBUG_LEVEL_MAX, "Requirement could not be satisfied %+v", requirement)
		}

	}
	return []transport.TransportEntity{}
}

// isDuplicateByWitness implements a local, anchor-sharded idempotency check using a Memory entity.
// It does NOT use any global index. The Memory node is created (if missing) with Context "Exec:<Action>:<Dep>"
// and Value=<signatureHash>. We link Anchor -> Memory for locality. If Memory already exists, we skip scheduling.
func (s *Scheduler) isDuplicateByWitness(actionName, depName string, input transport.TransportEntity, requirement transport.TransportEntity) bool {
	// Determine a deterministic anchor for this input
	anchor := s.selectAnchorForInput(input, requirement)
	// Build canonical signature string and hash it to keep Value compact
	sigStr := s.buildWitnessSignatureString(actionName, depName, anchor, input)
	sigHash := sha1.Sum([]byte(sigStr))
	sigHex := hex.EncodeToString(sigHash[:])
	ctx := fmt.Sprintf("Exec:%s:%s", actionName, depName)

	// Try to map (or match) Memory by Value using ID=-2 semantics
	memNode := s.memory.Mapper.MapTransportDataWithContext(transport.TransportEntity{
		Type:       "Memory",
		ID:         -2, // match by value if exists, else create
		Value:      sigHex,
		Context:    ctx,
		Properties: map[string]string{},
	}, "System")

	// If newly created, Mapper sets Properties["bMap"] = "" — then we must link Anchor->Memory and allow job creation
	if _, created := memNode.Properties["bMap"]; created {
		// Link anchor -> memory (best-effort; relationExists guard in storage avoids duplicates)
		s.linkAnchorToMemory(anchor, memNode)
		s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED WITNESS created ctx=", ctx, " val=", sigHex)
		return false
	}
	// Existing witness → duplicate
	s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED WITNESS exists ctx=", ctx, " val=", sigHex)
	return true
}

// selectAnchorForInput chooses a deterministic anchor entity from the constructed input.
// Preference: a Primary node Type from the dependency; else, the input root; as fallback, the lexicographically smallest (Type,ID) among input participants.
func (s *Scheduler) selectAnchorForInput(input transport.TransportEntity, requirement transport.TransportEntity) transport.TransportEntity {
	// try to find a Primary node type from the dependency structure
	var primaryTypes []string
	var walkReq func(n transport.TransportEntity)
	walkReq = func(n transport.TransportEntity) {
		if n.Type == "Structure" && n.Properties["Type"] == "Primary" {
			primaryTypes = append(primaryTypes, n.Value)
		}
		for _, ch := range n.Children() {
			walkReq(ch)
		}
	}
	if requirement.Type == "Dependency" && len(requirement.ChildRelations) > 0 {
		walkReq(requirement.Children()[0])
	} else {
		walkReq(requirement)
	}
	// try to find matching type in input graph
	if len(primaryTypes) > 0 {
		for _, pt := range primaryTypes {
			if hit, ok := s.findFirstInInputByType(&input, pt); ok {
				return *hit
			}
		}
	}
	// default to input root if it has a valid ID
	if input.ID > 0 {
		return input
	}
	// fallback: pick smallest (Type,ID) among all participants
	best := input
	s.rWalkInput(&input, func(e *transport.TransportEntity) {
		if e.ID > 0 {
			if e.Type < best.Type || (e.Type == best.Type && e.ID < best.ID) {
				best = *e
			}
		}
	})
	return best
}

// findFirstInInputByType returns the first occurrence of an entity with the given type in the input graph.
func (s *Scheduler) findFirstInInputByType(root *transport.TransportEntity, t string) (*transport.TransportEntity, bool) {
	var found *transport.TransportEntity
	s.rWalkInput(root, func(e *transport.TransportEntity) {
		if found == nil && e.Type == t && e.ID > 0 {
			found = e
		}
	})
	if found != nil {
		return found, true
	}
	return nil, false
}

// rWalkInput walks the transport entity graph (children and parents) and invokes fn for each node.
func (s *Scheduler) rWalkInput(e *transport.TransportEntity, fn func(*transport.TransportEntity)) {
	if e == nil {
		return
	}
	fn(e)
	for i := range e.ChildRelations {
		s.rWalkInput(&e.ChildRelations[i].Target, fn)
	}
	for i := range e.ParentRelations {
		s.rWalkInput(&e.ParentRelations[i].Target, fn)
	}
}

// buildWitnessSignatureString creates the canonical signature string used for Memory.Value.
func (s *Scheduler) buildWitnessSignatureString(actionName, depName string, anchor transport.TransportEntity, input transport.TransportEntity) string {
	base := actionName + "|" + depName + "|" + anchor.Type + ":" + strconv.Itoa(anchor.ID) + "|"
	return base + util.GenerateSignature(input)
}

// linkAnchorToMemory creates a relation between the anchor and the Memory node using Gits queries.
func (s *Scheduler) linkAnchorToMemory(anchor transport.TransportEntity, memoryNode transport.TransportEntity) {
	// Build query: Link <anchor.Type>[ID==anchor.ID] -> Memory[ID==memoryNode.ID]
	// Best effort; errors ignored here as storage guards against duplicates.
	if anchor.ID <= 0 || memoryNode.ID <= 0 {
		return
	}
	q := query.New().Link(anchor.Type).Match("ID", "==", strconv.Itoa(anchor.ID)).To(
		query.New().Find("Memory").Match("ID", "==", strconv.Itoa(memoryNode.ID)),
	)
	s.memory.Gits.Query().Execute(q)
}

// getOrCompilePattern returns a compiled pattern for the given action+dependency,
// building it once from the dependency tree and caching it. Read-only helper.
func (s *Scheduler) getOrCompilePattern(actionName string, dep transport.TransportEntity) *PatternNode {
	// Key by action + dependency ID; IDs are stable within type scope.
	key := actionName + "|" + strconv.Itoa(dep.ID)
	if pn, ok := s.patternCache[key]; ok {
		s.patternHits++
		s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling PATTERN cache hit key=", key, " hits=", s.patternHits, " misses=", s.patternMisses)
		return pn
	}
	s.patternMisses++
	s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling PATTERN cache miss key=", key, " hits=", s.patternHits, " misses=", s.patternMisses)
	// The dependency node has a single child which is the root Structure.
	var root transport.TransportEntity
	if dep.Type == "Dependency" && len(dep.ChildRelations) > 0 {
		root = dep.Children()[0]
	} else {
		root = dep
	}
	compiled := s.compilePatternNode(root)
	// validate duplicate aliases and log once per dependency key
	if s.hasDuplicateAliases(compiled) {
		s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling PATTERN duplicate-alias warning key=", key)
	}
	s.patternCache[key] = compiled
	// one-line summary once
	if !s.patternSummarized[key] {
		s.patternSummarized[key] = true
		slots := s.collectSlotLabels(compiled)
		matchCnt := s.countMatchNodes(compiled)
		s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling PATTERN summary root=", compiled.Type, " slots=", slots, " matchNodes=", matchCnt)
	}
	return compiled
}

// compilePatternNode compiles a transport dependency Structure node into a PatternNode.
func (s *Scheduler) compilePatternNode(n transport.TransportEntity) *PatternNode {
	// Collect filters from Properties where keys are Filter.<name>.(Field|Operator|Value)
	filters := map[string][3]string{}
	normalized := map[string]string{}
	for k, v := range n.Properties {
		if strings.HasPrefix(k, "Filter.") {
			parts := strings.Split(k, ".")
			if len(parts) == 3 {
				name := parts[1]
				kind := parts[2]
				rec := filters[name]
				switch kind {
				case "Field":
					rec[0] = v
					if strings.HasPrefix(v, "Properties.") {
						trimmed := strings.TrimPrefix(v, "Properties.")
						if trimmed != "" {
							normalized[trimmed] = v
						}
					}
				case "Operator":
					rec[1] = v
				case "Value":
					rec[2] = v
				}
				filters[name] = rec
			}
		}
	}
	// Deterministic child ordering: Alias (non-empty) -> Type -> original index
	type childWithIdx struct {
		e   transport.TransportEntity
		idx int
	}
	children := make([]childWithIdx, 0, len(n.ChildRelations))
	for i, ch := range n.Children() {
		children = append(children, childWithIdx{e: ch, idx: i})
	}
	sort.SliceStable(children, func(i, j int) bool {
		ai := children[i].e.Properties["Alias"]
		aj := children[j].e.Properties["Alias"]
		// Put non-empty alias before empty
		if (ai == "") != (aj == "") {
			return aj == "" && ai != "" // true if i has alias and j doesn't
		}
		if ai != aj {
			return ai < aj
		}
		// same alias (or both empty) -> order by Type
		if children[i].e.Value != children[j].e.Value {
			return children[i].e.Value < children[j].e.Value
		}
		// fallback: original index
		return children[i].idx < children[j].idx
	})
	var kids []*PatternNode
	for _, cw := range children {
		kids = append(kids, s.compilePatternNode(cw.e))
	}
	// Build node
	pn := &PatternNode{
		Alias:                  n.Properties["Alias"],
		Type:                   n.Value,
		Mode:                   n.Properties["Mode"],
		Filters:                filters,
		Children:               kids,
		NormalizedFilterFields: normalized,
	}
	return pn
}

// InvalidatePattern removes a compiled pattern from cache (used on re-registration).
func (s *Scheduler) InvalidatePattern(actionName string, depID int) {
	key := actionName + "|" + strconv.Itoa(depID)
	delete(s.patternCache, key)
	delete(s.patternSummarized, key)
	s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling PATTERN invalidated key=", key)
}

// hasDuplicateAliases traverses the compiled tree and returns true if any set of
// siblings has duplicate non-empty aliases.
func (s *Scheduler) hasDuplicateAliases(root *PatternNode) bool {
	dup := false
	var walk func(n *PatternNode)
	walk = func(n *PatternNode) {
		if n == nil {
			return
		}
		seen := map[string]bool{}
		for _, ch := range n.Children {
			if ch.Alias != "" {
				if seen[ch.Alias] {
					dup = true
				}
				seen[ch.Alias] = true
			}
		}
		for _, ch := range n.Children {
			walk(ch)
		}
	}
	walk(root)
	return dup
}

// collectSlotLabels returns child labels (alias or type) for summary logging.
func (s *Scheduler) collectSlotLabels(root *PatternNode) []string {
	if root == nil {
		return nil
	}
	labels := make([]string, 0, len(root.Children))
	for _, ch := range root.Children {
		if ch.Alias != "" {
			labels = append(labels, ch.Alias)
		} else {
			labels = append(labels, ch.Type)
		}
	}
	return labels
}

// countMatchNodes counts nodes whose Mode == "Match" in the compiled tree.
func (s *Scheduler) countMatchNodes(root *PatternNode) int {
	cnt := 0
	var walk func(n *PatternNode)
	walk = func(n *PatternNode) {
		if n == nil {
			return
		}
		if n.Mode == "Match" {
			cnt++
		}
		for _, ch := range n.Children {
			walk(ch)
		}
	}
	walk(root)
	return cnt
}

// DebugGetCompiledPattern exposes the compiled dependency pattern for tests only.
// It has no effect on scheduling behavior.
func (s *Scheduler) DebugGetCompiledPattern(cortex *Cortex, actionName, depName string) *PatternNode {
	act, err := cortex.GetAction(actionName)
	if err != nil {
		return nil
	}
	dep := act.GetDependencyByName(depName)
	return s.getOrCompilePattern(actionName, dep)
}

// hasRelevantFilter checks if any updated property key is used as a filter in the dependency structure
func (s *Scheduler) hasRelevantFilter(requirement transport.TransportEntity, updatedKeys string) bool {
	// Collect all filters from the requirement structure
	filters := make(map[string]bool)
	s.rCollectFilters(requirement, filters)

	// Check if any of the updated keys from the bMap exist in the filters
	updatedKeysSlice := strings.Split(updatedKeys, ",")
	for _, key := range updatedKeysSlice {
		trimmedKey := strings.TrimSpace(key)
		if filters[trimmedKey] {
			s.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling SCHED RELEVANCE matchedKey=", trimmedKey)
			return true
		}
	}
	return false
}

// rCollectFilters recursively collects all filter field names from the dependency structure
func (s *Scheduler) rCollectFilters(entity transport.TransportEntity, filters map[string]bool) {
	// Check if the current entity has filters
	for propKey, propVal := range entity.Properties {
		if strings.HasPrefix(propKey, "Filter.") && strings.HasSuffix(propKey, ".Field") {
			// Store the filter field as-is
			filters[propVal] = true
			// Also normalize fields that reference Properties.<Key> so that plain
			// updated property keys in bMap (which usually list just the key name)
			// still match the filter.
			// Example: Filter.X.Field = "Properties.RedirectTarget" -> also track "RedirectTarget"
			if strings.HasPrefix(propVal, "Properties.") {
				trimmed := strings.TrimPrefix(propVal, "Properties.")
				if trimmed != "" {
					filters[trimmed] = true
				}
			}
		}
	}

	// Recurse on child relations
	for _, childRelation := range entity.ChildRelations {
		s.rCollectFilters(childRelation.Target, filters)
	}

	// Recurse on parent relations
	for _, parentRelation := range entity.ParentRelations {
		s.rCollectFilters(parentRelation.Target, filters)
	}
}

func (s *Scheduler) enrichLookupAndPointerByRelationStructures(newRelationStructures map[string][2]*transport.TransportEntity, lookup map[string]int, pointer [][]*transport.TransportEntity) (map[string]int, [][]*transport.TransportEntity) {
	for _, entityPair := range newRelationStructures {
		for _, entity := range entityPair {
			if _, ok := lookup[entity.Type]; !ok {
				pointer = append(pointer, []*transport.TransportEntity{entity})
				lookup[entity.Type] = len(pointer) - 1
			}
		}

	}
	return lookup, pointer
}

func (s *Scheduler) enrichActionsAndDependenciesByNewRelationStructures(newRelationStructures map[string][2]*transport.TransportEntity, actionsAndDependencies [][2]string) [][2]string {
	for relationStructure, _ := range newRelationStructures {
		actions := s.retrieveActionsByRelationStructure(relationStructure)
		s.log.Debug(archivist.DEBUG_LEVEL_DUMP, "Retrieved actions by relationStructure "+relationStructure, actions)
		for _, action := range actions {
			add := true
			for _, val := range actionsAndDependencies {
				if val[0] == action[0] && val[1] == action[1] {
					add = false
				}
			}
			if add {
				actionsAndDependencies = append(actionsAndDependencies, action)
			}
		}
	}
	return actionsAndDependencies
}

// collectUpdatedEntityIDs walks the demultiplexed entity tree and the newRelationStructures
// to build a set of entity IDs that were updated/created in this mapper batch.
func (s *Scheduler) collectUpdatedEntityIDs(entity transport.TransportEntity, newRelationStructures map[string][2]*transport.TransportEntity) map[int]bool {
	ids := map[int]bool{}
	var walk func(e transport.TransportEntity)
	walk = func(e transport.TransportEntity) {
		if _, ok := e.Properties["bMap"]; ok {
			ids[e.ID] = true
		}
		for _, ch := range e.ChildRelations {
			walk(ch.Target)
		}
	}
	walk(entity)
	// relation-only updates: mark only the child endpoint as updated to avoid
	// sibling cross-trigger via a shared parent. The parent being updated would
	// cause unrelated sibling combinations to pass causality.
	for _, pair := range newRelationStructures {
		// pair[0] = parent, pair[1] = child
		if pair[1] != nil {
			ids[pair[1].ID] = true
		}
	}
	return ids
}

// inputContainsUpdated checks if the constructed job input contains any entity from the updated set.
func (s *Scheduler) inputContainsUpdated(input *transport.TransportEntity, updated map[int]bool) bool {
	found := false
	var walk func(e transport.TransportEntity)
	walk = func(e transport.TransportEntity) {
		if found {
			return
		}
		if updated[e.ID] {
			found = true
			return
		}
		for _, ch := range e.ChildRelations {
			walk(ch.Target)
			if found {
				return
			}
		}
	}
	walk(*input)
	return found
}

func (s *Scheduler) buildInputData(requirement transport.TransportEntity, lookup map[string]int, pointer [][]*transport.TransportEntity) []transport.TransportEntity {
	newJobs := []transport.TransportEntity{}
	qry := s.rBuildQuery(requirement, lookup, pointer)
	result := s.memory.Gits.Query().Execute(qry)

	if 0 < result.Amount {
		for _, enriched := range result.Entities {
			newJobs = append(newJobs, s.demultiplexer.Parse(enriched)...)
		}
	}
	return newJobs
}

func (s *Scheduler) rBuildQuery(requirement transport.TransportEntity, lookup map[string]int, pointer [][]*transport.TransportEntity) *query.Query {
	qry := query.New().Read(requirement.Value)
	// is requirement in index we add an exact ID matching filter
	if _, ok := lookup[requirement.Value]; ok {
		tmpEntity := pointer[lookup[requirement.Value]][0]
		qry.Match("ID", "==", strconv.Itoa(tmpEntity.ID))
	}
	// if its match mode we have to apply filters
	if requirement.Properties["Mode"] == "Match" {
		// we add match filters
		qry = s.enrichQueryFilters(qry, requirement)
	}
	// any child relations?
	if 0 < len(requirement.ChildRelations) {
		for _, childRelation := range requirement.ChildRelations {
			qry = qry.To(s.rBuildQuery(childRelation.Target, lookup, pointer))
		}
	}
	return qry
}

func (s *Scheduler) enrichQueryFilters(query *query.Query, requirement transport.TransportEntity) *query.Query {
	filters := make(map[string][]string)
	for name, val := range requirement.Properties {
		if len(name) > 6 && name[:6] == "Filter" {
			splitName := strings.Split(name, ".")
			// invalid structure
			if len(splitName) != 3 {
				s.log.Error("invalid filter format name: %s : skipping filter", name)
				continue // ### maybe should be handled different
			}
			key := splitName[1]
			typ := splitName[2]
			if _, ok := filters[key]; !ok {
				filters[key] = []string{"", "", ""}
			}
			switch typ {
			case "Field":
				filters[key][0] = val
			case "Operator":
				filters[key][1] = val
			case "Value":
				filters[key][2] = val
			}
		}
	}
	for _, val := range filters {
		query = query.Match(val[0], val[1], val[2])
	}
	return query
}

func (s *Scheduler) retrieveActionsByType(entityType string) [][2]string {
	var ret [][2]string
	qry := query.New().Read("DependencyEntityLookup").Match("Value", "==", entityType).To(
		query.New().Read("Dependency").From(
			query.New().Read("Action"),
		),
	)
	result := s.memory.Gits.Query().Execute(qry)
	s.log.Debug(archivist.DEBUG_LEVEL_DUMP, "DependencyEntityLookup ", entityType, result)
	if 0 < len(result.Entities) {
		for _, dependencyEntity := range result.Entities[0].Children() {

			for _, actionEntity := range dependencyEntity.Parents() { // ### todo : this is a very wierd behaviour, it would expect to also find the DependencyEntityLookup when checking the parents. but due to the way we build the return json tree its not
				ret = append(ret, [2]string{actionEntity.Value, dependencyEntity.Value})
			}
		}
	}
	return ret
}

func (s *Scheduler) retrieveActionsByRelationStructure(relationStructure string) [][2]string {
	var ret [][2]string
	qry := query.New().Read("DependencyRelationLookup").Match("Value", "==", relationStructure).To(
		query.New().Read("Dependency").From(
			query.New().Read("Action"),
		),
	)
	result := s.memory.Gits.Query().Execute(qry)
	s.log.Debug(archivist.DEBUG_LEVEL_DUMP, "DependencyRelationLookup ", relationStructure, result)
	if 0 < len(result.Entities) {
		for _, dependencyEntity := range result.Entities[0].Children() {
			for _, actionEntity := range dependencyEntity.Parents() { // ### todo : this is a very wierd behaviour, it would expect to also find the DependencyEntityLookup when checking the parents. but due to the way we build the return json tree its not
				ret = append(ret, [2]string{actionEntity.Value, dependencyEntity.Value})
			}
		}
	}
	return ret
}

func (s *Scheduler) rEnrichLookupAndPointer(entity transport.TransportEntity, lookup map[string]int, pointer [][]*transport.TransportEntity) (map[string]int, [][]*transport.TransportEntity) {
	s.log.Debug(archivist.DEBUG_LEVEL_MAX, "Enrichting step", entity)
	// lets see if this is newly learned data
	if _, ok := entity.Properties["bMap"]; ok {
		// do we already know about this entity type?
		if _, well := lookup[entity.Type]; !well {
			// it's not known, so we create wa whole new first level entry on pointer and
			// also add it to our lookup map for later use
			s.log.Debug(archivist.DEBUG_LEVEL_MAX, "Adding entity to pointer", entity)
			pointer = append(pointer, []*transport.TransportEntity{&entity})
			lookup[entity.Type] = len(pointer) - 1
		} else {
			// ### for now we gonne assume we only need the first upcome, later ones we skip. We might need to overthink
			// this since it hard impacts the scheduler (we would ne to multiplex on feeding into our dependency structure
			// in case there are same types on different levels. We keep this following line and else just in cae
			// we need to reactivate it. comments dont hurt
			//pointer[val] = append(pointer[val], &entity)
		}
	}
	for _, childRelation := range entity.ChildRelations {
		lookup, pointer = s.rEnrichLookupAndPointer(childRelation.Target, lookup, pointer)
	}
	//for _, parentRelation := range entity.ParentRelations { // ### enrichment towards parents is disabled for now
	// lookup, pointer = rEnrichLookupAndPointer(parentRelation.Target, lookup, pointer)
	//}
	return lookup, pointer
}
