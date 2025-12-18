package example

import (
	"github.com/voodooEntity/gits"
	"github.com/voodooEntity/gits/src/transport"
	"github.com/voodooEntity/cyberbrain/src/system/cerebrum"
	cfgb "github.com/voodooEntity/cyberbrain/src/system/configBuilder"
	"github.com/voodooEntity/cyberbrain/src/system/interfaces"
	"net"
	"strconv"
	"time"
)

type Example struct {
	Gits   *gits.Gits
	Mapper *cerebrum.Mapper
}

func New() interfaces.ActionInterface {
	tmp := &Example{}
	return tmp
}

func (self *Example) SetGits(gitsInstance *gits.Gits) {
	self.Gits = gitsInstance
}

func (self *Example) SetMapper(mapper *cerebrum.Mapper) {
	self.Mapper = mapper
}

// Execute method mandatory
func (self *Example) Execute(input transport.TransportEntity, requirement string, context string, jobUUID string) ([]transport.TransportEntity, error) {
	// resolve the ip
	ips, err := net.LookupIP(input.Value)
	if nil != err {
		// if there was en error, return no data and the error
		return []transport.TransportEntity{}, err
	}

	// for each IP resolved
	for _, ip := range ips {
		// for this example we only handle v4
		if ipv4 := ip.To4(); ipv4 != nil {
			// we enhance the input which provided the domain by the IP as child relation.
			// we use -2 flag to map (By Type and Value and Parent) ### add constants for this
			input.ChildRelations = append(input.ChildRelations, transport.TransportRelation{
				Target: transport.TransportEntity{
					ID:         -2,
					Type:       "IP",
					Value:      ipv4.String(),
					Context:    context,
					Properties: map[string]string{"protocol": "V4", "created": strconv.FormatInt(time.Now().Unix(), 10)},
				}})
		}
	}
	// make sure properties is well formed. this could be solved
	// better ###
	input.Properties = make(map[string]string)

	//
	time.Sleep(time.Second * 15)

	// now we return the enriched input data
	// which will automatically be mapped onto
	// the existing domain
	return []transport.TransportEntity{input}, nil
}

func (self *Example) GetConfig() transport.TransportEntity {
	// instance config and set base infos
	cfg := cfgb.NewConfig()
	cfg.SetName("resolveIPFromDomain")
	cfg.SetCategory("Pentest")

	// define a dependency
	alphaDependency := cfgb.NewStructure("Domain").SetPriority(cfgb.PRIORITY_PRIMARY).SetMode(cfgb.MODE_SET)

	// add dependency to config
	cfg.AddDependency("alpha", alphaDependency)

	// build the config format and return
	return cfg.Build()
}
