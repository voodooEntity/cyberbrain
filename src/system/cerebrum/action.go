package cerebrum

import (
	"github.com/voodooEntity/gits/src/transport"
	"github.com/voodooEntity/cyberbrain/src/system/interfaces"
)

type Action struct {
	name         string
	categories   []transport.TransportEntity
	dependencies []transport.TransportEntity
	instance     interfaces.ActionInterface
	factory      func() interfaces.ActionInterface
}

func NewAction() *Action {
	return &Action{}
}

func (self *Action) SetName(name string) *Action {
	self.name = name
	return self
}

func (self *Action) GetName() string {
	return self.name
}

func (self *Action) SetDependencies(dependencies []transport.TransportEntity) *Action {
	self.dependencies = dependencies
	return self
}

func (self *Action) GetDependencies() []transport.TransportEntity {
	return self.dependencies
}

func (self *Action) GetDependencyByName(name string) transport.TransportEntity {
	for _, dependency := range self.GetDependencies() {
		if name == dependency.Value {
			return dependency
		}
	}
	return transport.TransportEntity{}
}

func (self *Action) SetInstance(instance interfaces.ActionInterface) *Action {
	self.instance = instance
	return self
}
func (self *Action) GetInstance() interfaces.ActionInterface {
	return self.instance
}

func (self *Action) CreateInstance() interfaces.ActionInterface {
	return self.factory()
}

func (self *Action) SetCategories(categories []transport.TransportEntity) *Action {
	self.categories = categories
	return self
}

func (self *Action) GetCategories() []transport.TransportEntity {
	return self.categories
}

func (self *Action) SetFactory(f func() interfaces.ActionInterface) *Action {
	self.factory = f
	return self
}
