package observer

import (
	"time"

	"github.com/voodooEntity/gits"
	"github.com/voodooEntity/gits/src/query"
	"github.com/voodooEntity/cyberbrain/src/system/archivist"
	"github.com/voodooEntity/cyberbrain/src/system/cerebrum"
	"github.com/voodooEntity/cyberbrain/src/system/util"
)

type Observer struct {
	InactiveIncrement int
	memory            *cerebrum.Memory
	runnerAmount      int
	callback          func(memoryInstance *cerebrum.Memory)
	Runners           []Tracker
	lethal            bool
	log               *archivist.Archivist
	tickFunction      *func(gits *gits.Gits, logger *archivist.Archivist)
	tickRate          int
}

type Tracker struct {
	ID      int
	Version int
}

func New(memoryInstance *cerebrum.Memory, runnerAmount int, cb func(memoryInstance *cerebrum.Memory), logger *archivist.Archivist, lethal bool) *Observer {
	logger.Info("Creating observer")
	var runners []Tracker
	qry := query.New().Read("Neuron")
	res := memoryInstance.Gits.Query().Execute(qry)
	for _, val := range res.Entities {
		runners = append(runners, Tracker{
			ID:      val.ID,
			Version: val.Version,
		})
	}

	return &Observer{
		InactiveIncrement: 0,
		memory:            memoryInstance,
		Runners:           runners,
		callback:          cb,
		runnerAmount:      runnerAmount,
		lethal:            lethal,
		log:               logger,
		tickRate:          25,
		tickFunction:      nil,
	}
}

func (o *Observer) RegisterTickFunction(tickFn *func(gits *gits.Gits, logger *archivist.Archivist)) {
	o.tickFunction = tickFn
}

func (o *Observer) SetTickRate(tickRate int) {
	o.tickRate = tickRate
}

func (o *Observer) tick() {
	(*o.tickFunction)(o.memory.Gits, o.log)
}

func (o *Observer) Loop() {
	i := 0
	for !o.ReachedEndgame() {
		i++
		o.log.Debug(archivist.DEBUG_LEVEL_MAX, "Observer looping:")
		if nil != o.tickFunction && i == o.tickRate {
			o.tick()
			i = 0
		}

		time.Sleep(100 * time.Millisecond)
	}
	o.Endgame()
	o.log.Info("Cyberbrain has been shutdown, neuron exiting")
}

func (o *Observer) ReachedEndgame() bool {
    // If the system has been terminated externally (or by a timeout tick),
    // we should stop the observer loop immediately to avoid hanging forever.
    if !util.IsAlive(o.memory.Gits) {
        return true
    }
    runnerQry := query.New().Read("Neuron").Match("Properties.State", "==", "Searching")
    sysRunners := o.memory.Gits.Query().Execute(runnerQry)
    o.log.Debug(archivist.DEBUG_LEVEL_MAX, "Observer: searching neurons", sysRunners.Amount)
	o.log.Debug(archivist.DEBUG_LEVEL_MAX, "Observer: total amount created neurons", o.runnerAmount)
	openJobs := cerebrum.GetOpenJobs(o.memory.Gits)
	if openJobs.Amount == 0 && sysRunners.Amount == o.runnerAmount {
		changedVersion := false
		for _, sysRunner := range sysRunners.Entities {
			for tid, tracker := range o.Runners {
				if sysRunner.ID == tracker.ID {
					if sysRunner.Version != tracker.Version {
						changedVersion = true
						o.Runners[tid].Version = sysRunner.Version
					}
				}
			}
		}
		if changedVersion {
			o.InactiveIncrement = 0
			return false
		}
		if o.InactiveIncrement > 5 {
			return true
		}
		o.InactiveIncrement++
		return false
	}
	o.InactiveIncrement = 0
	return false
}

func (o *Observer) Endgame() {
	o.log.Info("executing endgame")
	// if we are lethal we gonne stop cyberbrain
	if o.lethal {
		util.Terminate(o.memory.Gits)
		for !o.AllNeuronDead() {
			time.Sleep(10 * time.Millisecond)
		}
	}
	// execute callback with memory instance provided
	o.callback(o.memory)
}

func (o *Observer) AllNeuronDead() bool {
	qry := query.New().Read("Neuron").Match("Properties.State", "==", "Dead")
	runners := o.memory.Gits.Query().Execute(qry)
	if runners.Amount == o.runnerAmount {
		return true
	}
	return false
}
