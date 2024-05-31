package main

import (
	"encoding/json"
	"log"
	"sync"

	uuid "github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TopoMsg struct {
	Type      string              `json:"type"`
	Topology  map[string][]string `json:"topology"`
	MessageID int                 `json:"msg_id"`
}

type server struct {
	n      *maelstrom.Node
	nodeId string

	msgMutex sync.RWMutex
	msgs     []int

	topoMutex sync.RWMutex
	topo      map[string][]string
}

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n, nodeId: n.ID()}

	n.Handle("echo", s.echoHandler)
	n.Handle("generate", s.generateHandler)
	n.Handle("broadcast", s.broadcastHandler)
	n.Handle("read", s.readHandler)
	n.Handle("topology", s.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// Challenge #1 : Echo - https://fly.io/dist-sys/1/
func (s *server) echoHandler(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Update the message type to return back.
	body["type"] = "echo_ok"

	// Echo the original message back with the updated message type.
	return s.n.Reply(msg, body)
}

// Challenge #2: Unique ID Generation - https://fly.io/dist-sys/2/
func (s *server) generateHandler(msg maelstrom.Message) error {
	// Since the ID may be of any-type, we use the uuid package to generate IDs
	return s.n.Reply(msg, map[string]any{
		"type": "generate_ok",
		"id":   uuid.New(),
	})
}

// Challenge #3: Broadcast - https://fly.io/dist-sys/3a/
func (s *server) topologyHandler(msg maelstrom.Message) error {
	var body TopoMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.topoMutex.Lock()
	s.topo = body.Topology
	s.topoMutex.Unlock()
	return s.n.Reply(msg, map[string]any{
		"type":        "topology_ok",
		"in_reply_to": body.MessageID,
	})
}

func (s *server) readHandler(msg maelstrom.Message) error {
	// Don't really need to handle the message body because we're only going to sending back all the messages that we've received
	s.msgMutex.RLock()
	messages := make([]int, len(s.msgs))
	for i := 0; i < len(s.msgs); i++ {
		messages[i] = s.msgs[i]
	}
	s.msgMutex.RUnlock()
	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	rcvd_message := int(body["message"].(float64))

	s.msgMutex.Lock()
	s.msgs = append(s.msgs, rcvd_message)
	s.msgMutex.Unlock()

	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}
