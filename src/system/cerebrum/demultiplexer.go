package cerebrum

import (
    "github.com/voodooEntity/gits/src/transport"
    "github.com/voodooEntity/cyberbrain/src/system/util"
)

type Demultiplexer struct {
}

func NewDemultiplexer() *Demultiplexer {
	return &Demultiplexer{}
}

func (d *Demultiplexer) Parse(entity transport.TransportEntity) []transport.TransportEntity {
    // prepare return data & some var initis
    var ret []transport.TransportEntity
    typeLookup := make(map[string]int)
    var typePointer [][]*transport.TransportEntity
    if 0 < len(entity.ChildRelations) {
		// collect children pointers grouped by type string
		for key := range entity.ChildRelations {
			if val, ok := typeLookup[entity.ChildRelations[key].Target.Type]; ok {
				typePointer[val] = append(typePointer[val], &(entity.ChildRelations[key].Target))
			} else {
				typePointer = append(typePointer, []*transport.TransportEntity{&(entity.ChildRelations[key].Target)})
				typeLookup[entity.ChildRelations[key].Target.Type] = len(typePointer) - 1
			}
		}

		// now we get the demultiplex each single one of them and build a second pointer list
		demultiplexedTypePointer := make([][]*transport.TransportEntity, len(typePointer))
		for typeId, typePointerList := range typePointer {
			for _, singlePointer := range typePointerList {
				demultiplexedTypePointer[typeId] = append(demultiplexedTypePointer[typeId], d.generateEntityPointerList(d.Parse(*singlePointer))...)
			}
		}

		// now we generate all possible recombinations in which each child entity Type  each type occurs once
		recombinations := d.generateRecombinations(demultiplexedTypePointer)

        for _, recombinationSet := range recombinations {
            var tmpChildren []transport.TransportRelation
            for key := range recombinationSet {
                // Deep-copy the target entity to guarantee immutability across combinations
                copied := d.deepCopyEntity(*recombinationSet[key])
                tmpChildren = append(tmpChildren, transport.TransportRelation{
                    Target: copied,
                })
            }
            ret = append(ret, transport.TransportEntity{
                Type:           entity.Type,
                ID:             entity.ID,
                Value:          entity.Value,
                Context:        entity.Context,
                Properties:     util.CopyStringStringMap(entity.Properties),
                ChildRelations: tmpChildren,
            })
        }
    } else {
        // Leaf: return a deep copy to ensure caller-side mutations do not affect siblings
        ret = append(ret, d.deepCopyEntity(entity))
    }
    return ret
}

func (d *Demultiplexer) generateEntityPointerList(data []transport.TransportEntity) []*transport.TransportEntity {
	var ret []*transport.TransportEntity
	for k := range data {
		ret = append(ret, &(data[k]))
	}
	return ret
}

func (d *Demultiplexer) generateRecombinations(data [][]*transport.TransportEntity) [][]*transport.TransportEntity {
	if len(data) == 0 {
		return [][]*transport.TransportEntity{}
	}

	var result [][]*transport.TransportEntity

	// Get the first row of values from data
	firstRow := data[0]

	// Recursively generate recombinations for the remaining rows
	remainingRows := d.generateRecombinations(data[1:])

	// If there are no remaining rows, return the first row as the only combination
	if len(remainingRows) == 0 {
		for _, val := range firstRow {
			result = append(result, []*transport.TransportEntity{val})
		}
		return result
	}

	// Combine the values from the first row with each recombination of the remaining rows
	for _, val := range firstRow {
		for _, comb := range remainingRows {
			result = append(result, append([]*transport.TransportEntity{val}, comb...))
		}
	}

	return result
}

// deepCopyEntity creates an independent copy of the provided transport entity,
// including a deep copy of Properties and all child/parent relations.
func (d *Demultiplexer) deepCopyEntity(src transport.TransportEntity) transport.TransportEntity {
    // Copy primitive fields and properties
    dst := transport.TransportEntity{
        Type:       src.Type,
        ID:         src.ID,
        Value:      src.Value,
        Context:    src.Context,
        Properties: util.CopyStringStringMap(src.Properties),
    }
    // Copy child relations recursively
    if len(src.ChildRelations) > 0 {
        dst.ChildRelations = make([]transport.TransportRelation, 0, len(src.ChildRelations))
        for _, cr := range src.ChildRelations {
            // Copy relation properties if present (shallow copy of map not necessary here as tests don't rely on it,
            // but we keep a safe copy pattern for completeness)
            var relProps map[string]string
            if cr.Properties != nil {
                relProps = util.CopyStringStringMap(cr.Properties)
            }
            dst.ChildRelations = append(dst.ChildRelations, transport.TransportRelation{
                Context:    cr.Context,
                Properties: relProps,
                Target:     d.deepCopyEntity(cr.Target),
            })
        }
    }
    // Copy parent relations recursively (rarely used in tests but included for completeness)
    if len(src.ParentRelations) > 0 {
        dst.ParentRelations = make([]transport.TransportRelation, 0, len(src.ParentRelations))
        for _, pr := range src.ParentRelations {
            var relProps map[string]string
            if pr.Properties != nil {
                relProps = util.CopyStringStringMap(pr.Properties)
            }
            dst.ParentRelations = append(dst.ParentRelations, transport.TransportRelation{
                Context:    pr.Context,
                Properties: relProps,
                Target:     d.deepCopyEntity(pr.Target),
            })
        }
    }
    return dst
}
