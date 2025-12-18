package scheduler

import (
	"log"
	"math/rand"
	"os"
	"strings"

	"github.com/voodooEntity/gits"
	"github.com/voodooEntity/gits/src/transport"
	"github.com/voodooEntity/cyberbrain/src/system/archivist"
	"github.com/voodooEntity/cyberbrain/src/system/cerebrum"
	"github.com/voodooEntity/cyberbrain/src/system/interfaces"
)

// - - - - - - - - - - - - - - - - - - - - - - -
// SETUP FRESH INSTANCE OF CYBERBRAIN
// - needs to be run for each test case
// - provides scheduler instance
// - provides memory instsance
// - provides cortex instance to have access to register to run schedule.Run
// - takes *transport.TransportEntity as input for db seed

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func setupFreshAndSeed(seedData *transport.TransportEntity, actions []func() interfaces.ActionInterface) (cerebrum.Scheduler, *cerebrum.Memory, *cerebrum.Cortex) {
	// first we create a new gits instance and set it as default
	instanceName := GenerateRandomString(10)
	gitsInstance := gits.NewInstance(instanceName)
	gits.SetDefault(instanceName)

	// setup the logger
	logger := archivist.New(&archivist.Config{Logger: log.New(os.Stdout, "", 0)})

	// now we setup the mapper with the fresh gits instance
	mapper := cerebrum.NewMapper(gitsInstance, logger)

	// if we got data to seed, we seed it
	if seedData != nil {
		mapper.MapTransportDataWithContext(*seedData, "Data")
	}

	// compose memory from mapper and gitsInstace
	mem := &cerebrum.Memory{
		Mapper: mapper,
		Gits:   gitsInstance,
	}

	// now we create some base data for cyberbrain
	createBaseData(mem)

	// finally we prepare our cortex with the actions
	// this should map all necessary config structures
	// into storage so scheduler can look them up
	cortex := prepareCortex(mem, logger, actions)

	// init scheduler with our fresh memory
	scheduler := getScheduler(mem, logger)

	// finally we return scheduler and respective memory
	// for the test case to run the scheduler, check the
	// results and beeing able to interact with memory
	return *scheduler, mem, cortex
}

func createBaseData(mem *cerebrum.Memory) {
	// create Open state
	mem.Gits.MapData(transport.TransportEntity{
		ID:         0,
		Type:       "State",
		Value:      "Open",
		Context:    "System",
		Properties: make(map[string]string),
	})
	// create Assigned state
	mem.Gits.MapData(transport.TransportEntity{
		ID:         0,
		Type:       "State",
		Value:      "Assigned",
		Context:    "System",
		Properties: make(map[string]string),
	})
	// create alife dataset
	properties := make(map[string]string)
	properties["State"] = "Alive"
	mem.Mapper.MapTransportData(transport.TransportEntity{
		Type:       "AI",
		Value:      "Cyberbrain",
		Context:    "System",
		Properties: properties,
	})
}

func prepareCortex(mem *cerebrum.Memory, logger *archivist.Archivist, actions []func() interfaces.ActionInterface) *cerebrum.Cortex {
	// we prepare the cortex in order to have the dependency entities of
	// actions beeing mapped into our storage so scheduler can look them up.
	// In case we gonne refactor the logic of mapping the dependencies for lookup
	// when rewriting the scheduler, we also might have to adjust the calls here
	cort := cerebrum.NewCortex(mem, logger)

	// register temporary actions instanaces in cortex
	for _, val := range actions {
		inst := val()
		instCfg := inst.GetConfig()
		instName := instCfg.Value
		cort.RegisterAction(instName, val)
	}
	return cort
}

func getScheduler(mem *cerebrum.Memory, logger *archivist.Archivist) *cerebrum.Scheduler {
	// create a demultiplexer instance, this might be removed later on refactoring
	// this makes it alot easier to later on adjust tests because we only need to update
	// this function.
	demultiplexer := cerebrum.NewDemultiplexer()

	// compose our scheduler instance and return it.
	scheduler := cerebrum.NewScheduler(mem, demultiplexer, logger)

	return scheduler
}

func GenerateRandomString(length int) string {
	// Create a strings.Builder to efficiently build the string
	var sb strings.Builder
	sb.Grow(length)

	// Loop 'length' times, selecting a random character from the charset
	for i := 0; i < length; i++ {
		// rand.Intn(n) returns a random int in the range [0, n)
		randomIndex := rand.Intn(len(charset))
		randomChar := charset[randomIndex]

		// Write the byte to the Builder
		sb.WriteByte(randomChar)
	}

	return sb.String()
}
