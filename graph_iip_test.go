package goflow

import (
	"testing"
)

func newRepeatGraph() (*Graph, error) {
	n := NewGraph()

	if err := n.Add("r", new(repeater)); err != nil {
		return nil, err
	}

	if err := n.MapInPort("Word", "r", "Word"); err != nil {
		return nil, err
	}
	if err := n.MapOutPort("Words", "r", "Words"); err != nil {
		return nil, err
	}

	return n, nil
}

func TestBasicIIP(t *testing.T) {
	p := new(pipeline)

	qty := 5

	input := "hello"
	output := []string{"hello", "hello", "hello", "hello", "hello"}

	in := make(chan string)
	out := make(chan string)

	var n *Graph

	p.
		ok(func() error {
			var err error
			n, err = newRepeatGraph()
			return err
		}).
		ok(func() error {
			return n.AddIIP("r", "Times", qty)
		}).
		ok(func() error {
			return n.SetInPort("Word", in)
		}).
		ok(func() error {
			return n.SetOutPort("Words", out)
		})
	if p.err != nil {
		t.Error(p.err)
		return
	}

	wait := Run(n)

	go func() {
		in <- input
		close(in)
	}()

	i := 0
	for actual := range out {
		expected := output[i]
		if actual != expected {
			t.Errorf("%s != %s", actual, expected)
		}
		i++
	}
	if i != qty {
		t.Errorf("Returned %d words instead of %d", i, qty)
	}

	<-wait
}

func newRepeatGraph2Ins() (*Graph, error) {
	p := new(pipeline)
	n := NewGraph()

	p.
		ok(func() error {
			return n.Add("r", new(repeater))
		}).
		ok(func() error {
			return n.MapInPort("Word", "r", "Word")
		}).
		ok(func() error {
			return n.MapInPort("Times", "r", "Times")
		}).
		ok(func() error {
			return n.MapOutPort("Words", "r", "Words")
		})
	if p.err != nil {
		return nil, p.err
	}

	return n, nil
}

func TestGraphInportIIP(t *testing.T) {
	qty := 5

	input := "hello"
	output := []string{"hello", "hello", "hello", "hello", "hello"}

	in := make(chan string)
	times := make(chan int)
	out := make(chan string)

	p := new(pipeline)

	var n *Graph

	p.
		ok(func() error {
			var err error
			n, err = newRepeatGraph2Ins()
			return err
		}).
		ok(func() error {
			return n.SetInPort("Word", in)
		}).
		ok(func() error {
			return n.SetInPort("Times", times)
		}).
		ok(func() error {
			return n.SetOutPort("Words", out)
		}).
		ok(func() error {
			return n.AddIIP("r", "Times", qty)
		})
	if p.err != nil {
		t.Error(p.err)
		return
	}

	wait := Run(n)

	go func() {
		in <- input
		close(in)
	}()

	i := 0
	for actual := range out {
		if i == 0 {
			// The graph inport needs to be closed once the IIP is sent
			close(times)
		}
		expected := output[i]
		if actual != expected {
			t.Errorf("%s != %s", actual, expected)
		}
		i++
	}
	if i != qty {
		t.Errorf("Returned %d words instead of %d", i, qty)
	}

	<-wait
}

func TestInternalConnectionIIP(t *testing.T) {
	input := 1
	iip := 2
	output := []int{1, 2}
	qty := 2

	in := make(chan int)
	out := make(chan int)

	p := new(pipeline)

	var n *Graph

	p.
		ok(func() error {
			var err error
			n, err = newDoubleEcho()
			return err
		}).
		ok(func() error {
			return n.AddIIP("e2", "In", iip)
		}).
		ok(func() error {
			return n.SetInPort("In", in)
		}).
		ok(func() error {
			return n.SetOutPort("Out", out)
		})
	if p.err != nil {
		t.Error(p.err)
		return
	}

	wait := Run(n)

	go func() {
		in <- input
		close(in)
	}()

	i := 0
	for actual := range out {
		// The order of output is not guaranteed in this case
		if actual != output[0] && actual != output[1] {
			t.Errorf("Unexpected value %d", actual)
		}
		i++
	}
	if i != qty {
		t.Errorf("Returned %d words instead of %d", i, qty)
	}

	<-wait
}

func TestAddRemoveIIP(t *testing.T) {
	n := NewGraph()

	p := new(pipeline)

	p.
		ok(func() error {
			return n.Add("e", new(echo))
		}).
		ok(func() error {
			return n.AddIIP("e", "In", 5)
		}).
		fails(func() error {
			return n.AddIIP("d", "No", 404)
		}).
		ok(func() error {
			return n.RemoveIIP("e", "In")
		}).
		fails(func() error {
			// Second attempt to remove same IIP should fail
			return n.RemoveIIP("e", "In")
		})
	if p.err != nil {
		t.Error(p.err)
	}
}
