package cerebrum

import (
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/voodooEntity/gits"
	"github.com/voodooEntity/gits/src/query"
	"github.com/voodooEntity/gits/src/transport"
	"github.com/voodooEntity/cyberbrain/src/system/archivist"
	"github.com/voodooEntity/cyberbrain/src/system/util"
)

// currently not in active use. ###
const (
	INTERCOM_BUFF_SIZE   int = 100000
	INTERCOM_INPUT_CHAN  int = 0
	INTERCOM_OUTPUT_CHAN int = 1
)

type Neuron struct {
	id       int
	uid      string
	intercom [2]chan string
	cortex   *Cortex
	job      Job
	memory   *Memory
	activity *Activity
	log      *archivist.Archivist
	history  bool
}

//   - - - - - - - - - - - - - - - - - - - - - -
//     Interface definitions placed here
//     to prevent cyclic imports - ###
//   - - - - - - - - - - - - - - - - - - - - - -
type ActionExtendGitsInterface interface {
	SetGits(*gits.Gits)
}

type ActionExtendMapperInterface interface {
	SetMapper(*Mapper)
}

type ActionExtendLoggerInterface interface {
	SetLogger(*archivist.Archivist)
}

func NewNeuron(id int, cortexInstance *Cortex, memoryInstance *Memory, activityInstance *Activity, logger *archivist.Archivist) *Neuron {
	logger.Info("Creating neuron", id)
	properties := make(map[string]string)
	properties["State"] = "Searching"
	memoryInstance.Mapper.MapTransportData(transport.TransportEntity{
		ID:         -1,
		Type:       "Neuron",
		Value:      strconv.Itoa(id),
		Context:    "Cyberbrain",
		Properties: properties,
	})

	return &Neuron{
		id:       id,
		intercom: [2]chan string{make(chan string, INTERCOM_BUFF_SIZE), make(chan string, INTERCOM_BUFF_SIZE)},
		cortex:   cortexInstance,
		memory:   memoryInstance,
		activity: activityInstance,
		log:      logger,
		history:  false,
	}
}

func (n *Neuron) Loop() {
	for util.IsAlive(n.memory.Gits) {
		n.log.Debug(archivist.DEBUG_LEVEL_MAX, "Neuron looping id: ", n.id)
		// lets try to assign a job
		if n.FindJob() {
			// now we gonne try execute the just assigned job
			results, err := n.ExecuteJob()
			// did it work out?
			if nil != err {
				n.FinishJobError(err)
				// ### todo think about how to handle errors
			} else {
				// now we map the data into our storage and run the result of the mapper
				// through our scheduler to create new Jobs based on what we just learned
				n.FinishJobSuccess(results)
			}
		} else {
			//			time.Sleep(1000000000)
			time.Sleep(100 * time.Millisecond)
		}
		//time.Sleep(time.Second * 4)
	}
	n.ChangeState("Dead")
	n.log.Info("Cyberbrain has been shutdown, neuron exiting")
}

func (n *Neuron) FindJob() bool {
	// query can be optimized by joining ###todo
	jobList := GetOpenJobs(n.memory.Gits)
	n.log.Debug(archivist.DEBUG_LEVEL_MAX, "Open Jobs found", jobList)
	// if there are any jobs
	if 0 < jobList.Amount {
		// iterate through them
		for _, jobEntity := range jobList.Entities[0].Parents() {
			// load the full job data as instance of job struct
			newJob := Load(jobEntity.ID, n.memory, n.log)
			if nil != newJob {
				// finally assign the job
				ok := n.AssignJob(newJob)
				if ok {
					return true
				}
			}
		}
	}
	// could not assign a job
	return false
}

func (n *Neuron) ExecuteJob() ([]transport.TransportEntity, error) {
	qry := query.New().Read("Neuron").Match("Value", "==", strconv.Itoa(n.id)).To(query.New().Read("Job").To(query.New().Read("Input")))
	ret := n.memory.Gits.Query().Execute(qry)

	if 0 == ret.Amount {
		return []transport.TransportEntity{}, errors.New("Neuron could not find any assigned job. This should be rather impossible")
	}

	// convert json input to actual struct instance
	inputJson := ret.Entities[0].Children()[0].Children()[0].Properties["Data"]
	var inputEntity transport.TransportEntity
	err := json.Unmarshal([]byte(inputJson), &inputEntity)
	if nil != err {
		n.log.Error("Job: "+ret.Entities[0].Children()[0].Value+" - could not convert job input json back to struct data", inputJson)
		return []transport.TransportEntity{}, err
	}

	// retrieve the action from taskRegistry and apply it ### handle error
	jobAction, _ := n.cortex.GetAction(ret.Entities[0].Children()[0].Properties["Action"])
	n.log.Info("Neuron " + strconv.Itoa(n.id) + " executing action " + jobAction.GetName() + " with Job " + ret.Entities[0].Children()[0].Value)

	// clear bMap properties from inputEntity, so we don't endless run
	rRemovebMap(inputEntity)

	// temporary debug ###
	jsonData, err := json.MarshalIndent(inputEntity, "", "\t")
	if err != nil {
		log.Fatalf("Error marshaling to JSON: %s", err)
	}
	n.log.Debug(archivist.DEBUG_LEVEL_DETAIL, "Job: "+ret.Entities[0].Children()[0].Value+" input data: "+string(jsonData))

	// retrieve an instance of the jobs action
	actionInstance := jobAction.GetInstance()

	// Check if the action accepts a Gits client
	if gitsSetter, ok := actionInstance.(ActionExtendGitsInterface); ok {
		gitsSetter.SetGits(n.memory.Gits)
	}

	// Check if the action accepts a Mapper
	if mapperSetter, ok := actionInstance.(ActionExtendMapperInterface); ok {
		mapperSetter.SetMapper(n.memory.Mapper)
	}

	// Check if the action accepts an Archivist
	if mapperSetter, ok := actionInstance.(ActionExtendLoggerInterface); ok {
		mapperSetter.SetLogger(n.log)
	}

	// and finally execute it
	results, err := actionInstance.Execute(inputEntity, ret.Entities[0].Children()[0].Properties["Requirement"], "Neuron", ret.Entities[0].Children()[0].Value)
	if nil != err {
		return []transport.TransportEntity{}, errors.New("Job: " + ret.Entities[0].Children()[0].Value + " execution failed with error " + err.Error())
	}
	n.log.Info("Job: " + ret.Entities[0].Children()[0].Value + " finished successfully")
	// temporary debug ###
	jsonData, err = json.MarshalIndent(results, "", "\t")
	if err != nil {
		log.Fatalf("Error marshaling to JSON: %s", err)
	}
	n.log.Debug(archivist.DEBUG_LEVEL_DETAIL, "Job: "+ret.Entities[0].Children()[0].Value+" finished with results: "+string(jsonData))

	return results, nil
}

func (n *Neuron) AssignJob(newJob *Job) bool {
	// update runners status to Assigning...
	n.ChangeState("Assigning")

	// letes see if we can assign that job
	ok := newJob.AssignToRunner(n.id)
	if !ok {
		// job could not be assigned, lets think about what reasons this could have
		// for different error handlings. for now we just gonne log it
		n.log.Debug(archivist.DEBUG_LEVEL_TRACE, "Neuron couldnt assign job: ", newJob.GetID())

		// update runners status...
		n.ChangeState("Searching")

		return false
	}

	// update runners status...
	n.ChangeState("Working")

	// ... link the job to the neuron
	qry := query.New().Find("Neuron").Match(
		"Value",
		"==",
		strconv.Itoa(n.id),
	).Link("Job").Match(
		"ID",
		"==",
		strconv.Itoa(newJob.GetID()),
	)
	n.memory.Gits.Query().Execute(qry)

	n.job = *newJob

	return true
}

func (n *Neuron) CheckChannel() {

}

func (n *Neuron) GetInputIntercom() chan string {
	return n.intercom[INTERCOM_INPUT_CHAN]
}

func (n *Neuron) GetOutputIntercom() chan string {
	return n.intercom[INTERCOM_OUTPUT_CHAN]
}

func (n *Neuron) ChangeState(state string) {
	qry := query.New().Update("Neuron").Match(
		"Value",
		"==",
		strconv.Itoa(n.id),
	).Set(
		"Properties.State",
		state,
	)
	n.memory.Gits.Query().Execute(qry)
}

func (n *Neuron) FinishJobSuccess(results []transport.TransportEntity) {
    // going through the results
    for _, result := range results {
        n.log.Debug(archivist.DEBUG_LEVEL_MAX, "Mapping result from job", result)
        mappedResult := n.memory.Mapper.MapTransportData(result)
        // temporary debug ###
        jsonData, err := json.MarshalIndent(mappedResult, "", "\t")
        if err != nil {
            log.Fatalf("Error marshaling to JSON: %s", err)
        }
        n.log.Debug(archivist.DEBUG_LEVEL_DETAIL, "Running freshly mapped job return with scheduler: "+string(jsonData))
        // scheduling: log origin neuron and signature before scheduling
        n.log.Debug(archivist.DEBUG_LEVEL_TRACE, "scheduling NEURON ", n.id, " scheduling result signature=", util.GenerateSignature(mappedResult))
        n.activity.Scheduler.Run(result, n.cortex)
    }

	qry := query.New().Read("Neuron").Match(
		"Value",
		"==",
		strconv.Itoa(n.id),
	).To(query.New().Read("Job"))

	runnerWithJob := n.memory.Gits.Query().Execute(qry)
	jobId := runnerWithJob.Entities[0].Children()[0].ID
	n.log.Debug(archivist.DEBUG_LEVEL_DUMP, "Detaching job from neuron", runnerWithJob)
	qry = query.New().Unlink("Neuron").Match("Value", "==", strconv.Itoa(n.id)).To(
		query.New().Find("Job").Match("ID", "==", strconv.Itoa(jobId)),
	)
	n.memory.Gits.Query().Execute(qry)

	if !n.history {
		n.deleteJobAndInput(jobId)
	} else {
		n.deleteJobInput(jobId)
	}

	n.ChangeState("Searching")
}

func (n *Neuron) FinishJobError(err error) {
	n.log.Info("Ended job with error: ", err.Error())
	qry := query.New().Read("Neuron").Match(
		"Value",
		"==",
		strconv.Itoa(n.id),
	).To(query.New().Read("Job"))

	runnerWithJob := n.memory.Gits.Query().Execute(qry)
	jobId := runnerWithJob.Entities[0].Children()[0].ID
	n.log.Debug(archivist.DEBUG_LEVEL_DUMP, "Detaching job from neuron", runnerWithJob)
	qry = query.New().Unlink("Neuron").Match("Value", "==", strconv.Itoa(n.id)).To(
		query.New().Find("Job").Match("ID", "==", strconv.Itoa(jobId)),
	)

	n.memory.Gits.Query().Execute(qry)

	if !n.history {
		n.deleteJobAndInput(jobId)
	} else {
		n.deleteJobInput(jobId)
	}

	n.ChangeState("Searching")
}

func (n *Neuron) deleteJobAndInput(jobID int) {
	jobQry := query.New().Read("Job").Match("ID", "==", strconv.Itoa(jobID)).To(
		query.New().Read("Input"),
	)
	dat := n.memory.Gits.Query().Execute(jobQry)

	unlinkQuery := query.New().Unlink("Job").Match("Value", "==", strconv.Itoa(dat.Entities[0].ID)).To(
		query.New().Find("Input").Match("ID", "==", strconv.Itoa(dat.Entities[0].Children()[0].ID)),
	)
	n.memory.Gits.Query().Execute(unlinkQuery)

	jobDeleteQry := query.New().Delete("Job").Match("ID", "==", strconv.Itoa(dat.Entities[0].ID))
	n.memory.Gits.Query().Execute(jobDeleteQry)

	inputDeleteQuery := query.New().Delete("Input").Match("ID", "==", strconv.Itoa(dat.Entities[0].Children()[0].ID))
	n.memory.Gits.Query().Execute(inputDeleteQuery)

}

func (n *Neuron) deleteJobInput(jobID int) {
	inputQry := query.New().Read("Input").From(
		query.New().Read("Job").Match("ID", "==", strconv.Itoa(jobID)),
	)
	dat := n.memory.Gits.Query().Execute(inputQry)

	inputDeleteQry := query.New().Delete("Input").Match("ID", "==", strconv.Itoa(dat.Entities[0].ID))
	n.memory.Gits.Query().Execute(inputDeleteQry)
}

func (n *Neuron) EnableHistory() {
	n.history = true
}

func rRemovebMap(entity transport.TransportEntity) {
	if _, ok := entity.Properties["bMap"]; ok {
		delete(entity.Properties, "bMap")
	}
	for _, val := range entity.ChildRelations {
		rRemovebMap(val.Target)
	}
	for _, val := range entity.ParentRelations {
		rRemovebMap(val.Target)
	}
}
