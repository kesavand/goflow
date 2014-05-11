package flow

import (
	"code.google.com/p/go.net/websocket"
	"log"
	"net"
	"net/http"
)

// Message represents a single FBP protocol message
type Message struct {
	// Protocol is NoFlo protocol identifier:
	// "runtime", "component", "graph" or "network"
	Protocol string
	// Command is a command to be executed within the protocol
	Command string
	// Payload is JSON-encoded body of the message
	Payload interface{}
}

type protocolHandler func(*websocket.Conn, interface{})

// Runtime is a NoFlo-compatible runtime implementing the FBP protocol
type Runtime struct {
	handlers map[string]protocolHandler
	ready    chan struct{}
	done     chan struct{}
}

func (r *Runtime) runtimeGetRuntime(ws *websocket.Conn, payload interface{}) {
	websocket.JSON.Send(ws, struct {
		Type         string
		Version      string
		Capabilities []string
	}{"goflow",
		"0.4",
		[]string{"protocol:runtime", "protocol:graph", "protocol:component"},
	})
}

// Register command handlers
func (r *Runtime) Init() {
	r.done = make(chan struct{})
	r.ready = make(chan struct{})
	r.handlers = make(map[string]protocolHandler)
	r.handlers["runtime.getruntime"] = r.runtimeGetRuntime
}

// Ready returns a channel which is closed when the runtime is ready to work
func (r *Runtime) Ready() chan struct{} {
	return r.ready
}

// Stop tells the runtime to shut down
func (r *Runtime) Stop() {
	close(r.done)
}

func (r *Runtime) Handle(ws *websocket.Conn) {
	defer func() {
		err := ws.Close()
		if err != nil {
			log.Println(err.Error())
		}
	}()
	var msg Message
	if err := websocket.JSON.Receive(ws, &msg); err != nil {
		log.Println(err.Error())
		return
	}
	handler, exists := r.handlers[msg.Protocol+"."+msg.Command]
	if !exists {
		log.Printf("Unknown command: %s.%s\n", msg.Protocol, msg.Command)
		return
	}
	handler(ws, msg.Payload)
}

func (r *Runtime) Listen(address string) {
	http.Handle("/", websocket.Handler(r.Handle))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalln(err.Error())
	}

	go func() {
		err = http.Serve(listener, nil)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}()
	close(r.ready)

	// Wait for termination signal
	<-r.done
	listener.Close()
}
