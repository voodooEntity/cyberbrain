package cerebrum

import (
	"errors"

	"github.com/voodooEntity/gits/src/query"
	"github.com/voodooEntity/gits/src/transport"
	"github.com/voodooEntity/cyberbrain/src/system/archivist"
	"github.com/voodooEntity/cyberbrain/src/system/interfaces"
	"github.com/voodooEntity/cyberbrain/src/system/util"
)

type Cortex struct {
	register map[string]*Action
	memory   *Memory
	log      *archivist.Archivist
}

func NewCortex(memoryInstance *Memory, logger *archivist.Archivist) *Cortex {
	return &Cortex{
		register: make(map[string]*Action),
		memory:   memoryInstance,
		log:      logger,
	}
}

func (c *Cortex) RegisterAction(name string, factory func() interfaces.ActionInterface) {
	instance := factory()

	// store action config
	c.memory.Mapper.MapTransportDataWithContext(instance.GetConfig(), "System")

	// Get the mapped categories
	catQry := query.New().Read("Action").Match("Value", "==", name).To(query.New().Read("Category").TraverseOut(10))
	categories := c.memory.Gits.Query().Execute(catQry)

	// Get the mapped dependencies
	depQry := query.New().Read("Action").Match("Value", "==", name).To(query.New().Read("Dependency").TraverseOut(10))
	dependencies := c.memory.Gits.Query().Execute(depQry)

	// create an action struct instance satisfied with the just mapped config and dependency data & the actual action instance itself
	actionInstance := *NewAction().SetName(name).SetDependencies(dependencies.Entities[0].Children()).SetCategories(categories.Entities[0].Children()).SetInstance(instance).SetFactory(factory)

	// recursive filter all upcoming dependency types and map them onto lookup nodes for further faster processing
	for _, val := range actionInstance.GetDependencies() {
		dependencyTypeList := c.getDependencyStructureTypes(val)
		c.log.Debug(archivist.DEBUG_LEVEL_DETAIL, "Mapping dependency lookup ", dependencyTypeList, val.ID)
		c.mapDependencyEntityLookupNodes(dependencyTypeList, val.ID)
		c.mapDependencyRelationLookupNodes(val)
	}

	// finally we place the module instance inside our map
	c.register[name] = &actionInstance

	c.log.Info("Registered action ", name)

}

func (c Cortex) GetAction(name string) (*Action, error) {
	if val, ok := c.register[name]; ok {
		return val, nil
	}
	return nil, errors.New("Action '" + name + "'not found in cortex")
}

func (c Cortex) GetInstance(name string) (interfaces.ActionInterface, error) {
	if val, ok := c.register[name]; ok {
		return val.CreateInstance(), nil
	}
	return nil, errors.New("Action '" + name + "'not found in cortex")
}

func (c Cortex) mapDependencyRelationLookupNodes(entity transport.TransportEntity) {
	var relationStructures []string
	relationStructures = c.rFindRelationStructures(entity, relationStructures)
	c.log.Debug(archivist.DEBUG_LEVEL_DUMP, "Relation structures found in cortex ", relationStructures)
	for _, val := range relationStructures {
		c.memory.Gits.MapData(transport.TransportEntity{
			Type:       "DependencyRelationLookup",
			ID:         0,
			Value:      val,
			Context:    "System",
			Properties: make(map[string]string),
			ChildRelations: []transport.TransportRelation{
				transport.TransportRelation{
					Context: "Structure",
					Target: transport.TransportEntity{
						Type: "Dependency",
						ID:   entity.ID,
					},
				},
			},
		})
	}
}

func (c Cortex) rFindRelationStructures(entity transport.TransportEntity, relationStructures []string) []string {
	if 0 < len(entity.ChildRelations) {
		for _, childRelation := range entity.ChildRelations {
			tmpRelString := entity.Value + "-" + childRelation.Target.Value
			add := true
			for _, knownRelString := range relationStructures {
				if knownRelString == tmpRelString {
					add = false
				}
			}
			if add {
				relationStructures = append(relationStructures, tmpRelString)
			}
			relationStructures = c.rFindRelationStructures(childRelation.Target, relationStructures)
		}
	}
	return relationStructures
}

func (c Cortex) mapDependencyEntityLookupNodes(dependencyTypes []string, dependencyId int) {
	for _, val := range dependencyTypes {
		c.memory.Gits.MapData(transport.TransportEntity{
			Type:       "DependencyEntityLookup",
			ID:         0,
			Value:      val,
			Context:    "System",
			Properties: make(map[string]string),
			ChildRelations: []transport.TransportRelation{
				transport.TransportRelation{
					Context: "Structure",
					Target: transport.TransportEntity{
						Type: "Dependency",
						ID:   dependencyId,
					},
				},
			},
		})
	}
}

func (c Cortex) getDependencyStructureTypes(entity transport.TransportEntity) []string {
	var typeList []string
	c.rGetTypeList(&typeList, []transport.TransportEntity{entity})
	return typeList
}

func (c Cortex) rGetTypeList(typeList *[]string, data []transport.TransportEntity) {
	for _, val := range data {
		// ### refactor if type changes , context should be structure so we gonne name this structure
		// but with context structurethis should be fiitting for all cases prolly i dunno. Also we check if type
		// is Primary, because only a Primary should trigger a new job , just having an state:open should not trigger
		// everything that might filter for a state. tho if its a Port with state open the Port should trigger. Thatfor
		// dependency structures should be marked as Primary or Secondary
		if !util.StringInArray(*typeList, val.Value) && "Structure" == val.Type && val.Properties["Type"] == "Primary" {
			*typeList = append(*typeList, val.Value)
		}
		if 0 < len(val.ChildRelations) {
			c.rGetTypeList(typeList, val.Children())
		}
	}
}
