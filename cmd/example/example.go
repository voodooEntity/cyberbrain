package main

import (
	"fmt"
	"github.com/voodooEntity/gits"
	"github.com/voodooEntity/gits/src/storage"
	"os"

	"github.com/voodooEntity/gits/src/transport"
	"github.com/voodooEntity/cyberbrain"
	"github.com/voodooEntity/cyberbrain/src/example"
	"github.com/voodooEntity/cyberbrain/src/system/archivist"
	"github.com/voodooEntity/cyberbrain/src/system/cerebrum"
	"log"
)

func main() {
	//logger := log.New(io.Discard, "", 0)
	logger := log.New(os.Stdout, "", 0)

	// create base instance. ident is required.
	// NeuronAmount will default back to
	// runtime.NumCPU == num logical cpu's
	cb := cyberbrain.New(cyberbrain.Settings{
		NeuronAmount: 1,
		Ident:        "GreatName",
		LogLevel:     archivist.LEVEL_INFO,
		Logger:       logger,
		History:      true,
	})

	// register actions
	cb.RegisterAction("resolveIPFromDomain", example.New)

	// start the neurons
	cb.Start()

	// Learn data and schedule based on it
	cb.LearnAndSchedule(transport.TransportEntity{
		ID:         storage.MAP_FORCE_CREATE,
		Type:       "Domain",
		Value:      "laughingman.dev",
		Context:    "example code",
		Properties: map[string]string{},
	})

	// get an observer instance. provide a callback
	// to be executed at the end and lethal=true
	// which stops the cyberbrain at the end
	obsi := cb.GetObserverInstance(func(mi *cerebrum.Memory) {
		qry := mi.Gits.Query().New().Read("IP")
		ret := mi.Gits.Query().Execute(qry)
		logger.Println("Result:", ret)
	}, true)

	// register a tick function
	fn := func(gits *gits.Gits, logger *archivist.Archivist) {
		logger.Info("yes i tick")
	}
	obsi.RegisterTickFunction(&fn)
	obsi.SetTickRate(20)

	// blocking while neurons are
	// working & non-finished jobs exist
	obsi.Loop()

	// history is enabled so we can lookup the
	// executed jobs
	qry := gits.NewQuery().Read("Job")
	res := gits.GetDefault().Query().Execute(qry)
	fmt.Println(fmt.Sprintf("%+v", res))
}
