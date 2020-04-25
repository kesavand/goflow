package goflow

import (
	"testing"
)

func TestFactoryCreate(t *testing.T) {
	f := NewFactory()
	err := RegisterTestComponents(f)
	if err != nil {
		t.Error(err)
		return
	}

	instance, err := f.Create("echo")
	if err != nil {
		t.Error(err)
		return
	}
	c, ok := instance.(Component)
	if !ok {
		t.Errorf("%+v is not a Component", c)
		return
	}

	_, err = f.Create("notfound")
	if err == nil {
		t.Errorf("Expected an error")
	}
}

func TestFactoryRegistration(t *testing.T) {
	p := new(pipeline)

	f := NewFactory(FactoryConfig{
		RegistryCapacity: 10,
	})

	p.
		ok(func() error {
			return RegisterTestComponents(f)
		}).
		fails(func() error {
			return f.Annotate("notfound", Annotation{})
		}).
		ok(func() error {
			return f.Unregister("echo")
		}).
		fails(func() error {
			return f.Unregister("echo")
		})

	if p.err != nil {
		t.Error(p.err)
	}
}

func TestFactoryGraph(t *testing.T) {
	p := new(pipeline)

	f := NewFactory()

	p.
		ok(func() error {
			return RegisterTestComponents(f)
		}).
		ok(func() error {
			return RegisterTestGraph(f)
		})
	if p.err != nil {
		t.Error(p.err)
		return
	}

	n := NewGraph()

	p.
		ok(func() error {
			return n.AddNew("de", "doubleEcho", f)
		}).
		ok(func() error {
			return n.AddNew("e", "echo", f)
		}).
		fails(func() error {
			return n.AddNew("notfound", "notfound", f)
		}).
		ok(func() error {
			return n.Connect("de", "Out", "e", "In")
		})

	n.MapInPort("In", "de", "In")
	n.MapOutPort("Out", "e", "Out")

	if p.err != nil {
		t.Error(p.err)
		return
	}

	testGraphWithNumberSequence(n, t)
}
