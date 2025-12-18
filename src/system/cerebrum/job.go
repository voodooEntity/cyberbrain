package cerebrum

import (
	"encoding/json"
	"strconv"

	"github.com/voodooEntity/gits"
	"github.com/voodooEntity/gits/src/query"
	"github.com/voodooEntity/gits/src/transport"
	gitsTypes "github.com/voodooEntity/gits/src/types"
	"github.com/voodooEntity/cyberbrain/src/system/archivist"
	"github.com/voodooEntity/cyberbrain/src/system/util"
)

type Job struct {
	data   transport.TransportEntity
	memory *Memory
	id     int
	log    *archivist.Archivist
}

func NewJob(memoryInstance *Memory, logger *archivist.Archivist) *Job {
	return &Job{
		memory: memoryInstance,
		log:    logger,
	}
}

func (j *Job) Create(action string, requirement string, input transport.TransportEntity) *Job {
	jobProperties := make(map[string]string)
	jobProperties["Action"] = action
	jobProperties["Requirement"] = requirement
	inputProperties := make(map[string]string)
	inputJson, err := json.Marshal(input)
	if nil != err {
		j.log.Error("Could not create json input payload from input string", input)
		return &Job{}
	}
	inputProperties["Data"] = string(inputJson)

	// refer to existing state:open instead of mapping like this since we use parent as related check and
	// parent is new created
	mapped := j.memory.Mapper.MapTransportDataWithContextForceCreate(transport.TransportEntity{
		ID:         -1,
		Type:       "Job",
		Context:    "system",
		Value:      util.UniqueID(),
		Properties: jobProperties,
		ChildRelations: []transport.TransportRelation{
			{
				Target: transport.TransportEntity{
					Type:       "Input",
					ID:         -1,
					Context:    "Cyberbrain",
					Value:      util.UniqueID(),
					Properties: inputProperties,
				},
			},
		},
	}, "System")

	openState := j.memory.Mapper.MapTransportData(transport.TransportEntity{
		Type:       "State",
		Value:      "Open",
		Context:    "System",
		ID:         0,
		Properties: make(map[string]string),
	})

	linkQuery := query.New().Link("Job").Match("ID", "==", strconv.Itoa(mapped.ID)).To(
		query.New().Find("State").Match("ID", "==", strconv.Itoa(openState.ID)),
	)

	j.memory.Gits.Query().Execute(linkQuery)

	j.log.Debug(archivist.DEBUG_LEVEL_INFO, "Mapped new job", mapped)
	return &Job{
		id: mapped.ID,
	}
}

func Load(id int, memoryInstance *Memory, logger *archivist.Archivist) *Job {
	// make sure that job actually exists
	ret := memoryInstance.Gits.Query().Execute(query.New().Read("Job").Match("ID", "==", strconv.Itoa(id)).TraverseOut(30)) // ### add max depth config for job
	if 0 < ret.Amount {
		return &Job{
			id:     id,
			data:   ret.Entities[0],
			memory: memoryInstance,
			log:    logger,
		}
	}
	return nil
}

func (j *Job) AssignToRunner(runnerID int) bool {
	// since we have to make sure we dont run into race conditions we gonne do some direct
	// api calls into gits here. we may change this at some point to query logics or something else
	// its not bad by design it just could be done smoother ###todo recheck if this cant be solved by qry where conditions
	j.memory.Gits.Storage().EntityStorageMutex.Lock()
	j.memory.Gits.Storage().RelationStorageMutex.Lock()
	jobTypeID, err := j.memory.Gits.Storage().GetTypeIdByStringUnsafe("Job")
	if nil != err {
		// this shouldn't be happening but rather handle every error than assume its impossible
		j.log.Debug(archivist.DEBUG_LEVEL_TRACE, "The impossible occured. Run you fools")
		j.memory.Gits.Storage().EntityStorageMutex.Unlock()
		j.memory.Gits.Storage().RelationStorageMutex.Unlock()
		return false
	}

	e, err := j.memory.Gits.Storage().GetEntityByPathUnsafe(jobTypeID, j.data.ID, "")
	if nil != err {
		j.memory.Gits.Storage().EntityStorageMutex.Unlock()
		j.memory.Gits.Storage().RelationStorageMutex.Unlock()
		j.log.Debug(archivist.DEBUG_LEVEL_INFO, "Runner tries to assign nonexisting job", j.data.ID)
		return false
	}
	j.data = transport.TransportEntity{
		Type:       "Job",
		ID:         e.ID,
		Context:    e.Context,
		Properties: e.Properties,
		Version:    e.Version,
	}

	childRelations, _ := j.memory.Gits.Storage().GetChildRelationsBySourceTypeAndSourceIdUnsafe(jobTypeID, e.ID, "")
	stateTypeID, _ := j.memory.Gits.Storage().GetTypeIdByStringUnsafe("State")
	for _, childRelation := range childRelations {
		if stateTypeID == childRelation.TargetType {
			openState, _ := j.memory.Gits.Storage().GetEntityByPathUnsafe(stateTypeID, childRelation.TargetID, "")
			j.log.Debug(archivist.DEBUG_LEVEL_TRACE, "state retrieved from job", openState)
			if "Open" != openState.Value {
				j.memory.Gits.Storage().EntityStorageMutex.Unlock()
				j.memory.Gits.Storage().RelationStorageMutex.Unlock()
				j.log.Debug(archivist.DEBUG_LEVEL_INFO, "Runner tries to assign job that state is not Open", j.data.ID)
				return false
			}
			// detach open state from job
			j.memory.Gits.Storage().DeleteRelationUnsafe(e.Type, e.ID, stateTypeID, openState.ID)
			//gits.DeleteEntityUnsafe(openState.Type, openState.ID)
			// get assigned state entity
			assignedState, _ := j.memory.Gits.Storage().GetEntitiesByTypeAndValueUnsafe("State", "Assigned", "match", "System")
			j.log.Debug(archivist.DEBUG_LEVEL_DUMP, "assigned state entity", assignedState)
			// now we map the job to the assigned entity
			j.memory.Gits.Storage().CreateRelationUnsafe(e.Type, e.ID, stateTypeID, assignedState[0].ID, gitsTypes.StorageRelation{
				SourceType: jobTypeID,
				SourceID:   e.ID,
				TargetType: stateTypeID,
				TargetID:   assignedState[0].ID,
				Context:    "Cyberbrain",
				Properties: make(map[string]string),
			})
		}
	}

	// finally we assign the job to the neuron
	//runnerTypeID, _ := gits.GetTypeIdByStringUnsafe("Runner")
	runnerEntity, _ := j.memory.Gits.Storage().GetEntitiesByTypeAndValueUnsafe("Neuron", strconv.Itoa(runnerID), "match", "Cyberbrain")
	j.log.Debug(archivist.DEBUG_LEVEL_DETAIL, "Map neuron to job", runnerEntity[0].Type, runnerEntity[0].ID, jobTypeID, e.ID)
	j.memory.Gits.Storage().CreateRelationUnsafe(runnerEntity[0].Type, runnerEntity[0].ID, jobTypeID, e.ID, gitsTypes.StorageRelation{
		SourceType: runnerEntity[0].Type,
		SourceID:   runnerEntity[0].ID,
		TargetType: jobTypeID,
		TargetID:   e.ID,
		Context:    "Cyberbrain",
		Properties: make(map[string]string),
	})

	j.memory.Gits.Storage().EntityStorageMutex.Unlock()
	j.memory.Gits.Storage().RelationStorageMutex.Unlock()
	return true
}

func (j *Job) GetState() string {
	ret := j.memory.Gits.Query().Execute(query.New().Read("Job").Match("ID", "==", strconv.Itoa(j.data.ID)).To(query.New().Read("State")))
	if 0 < ret.Amount {
		return ret.Entities[0].Children()[0].Value
	}
	j.log.Error("Retrieving state of an non existing job , should actually not happen, jobid is ", j.GetID())
	return ""
}

func (self *Job) GetID() int {
	return self.data.ID
}

func GetOpenJobs(gitsInstance *gits.Gits) transport.Transport {
	qry := query.New().Read("State").Match("Value", "==", "Open").Match("Context", "==", "System").From(
		query.New().Read("Job").Match("Context", "==", "System"))
	return gitsInstance.Query().Execute(qry)
}
