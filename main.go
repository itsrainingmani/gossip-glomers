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

// type BroadcastMsg struct {
// 	Type      string `json:"type"`
// 	Message   int    `json:"message"`
// 	MessageID int    `json:"msg_id"`
// }

type server struct {
	n         *maelstrom.Node
	neighbors []string

	msgMutex sync.RWMutex
	msgs     map[int]bool
}

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n, msgs: make(map[int]bool)}

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

	s.neighbors = body.Topology[s.n.ID()]
	return s.n.Reply(msg, map[string]any{
		"type":        "topology_ok",
		"in_reply_to": body.MessageID,
	})
}

func (s *server) readHandler(msg maelstrom.Message) error {
	// Don't really need to handle the message body because we're only going to send back all the messages that we've received
	s.msgMutex.RLock()
	messages := make([]int, 0, len(s.msgs))
	for id := range s.msgs {
		messages = append(messages, id)
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
	if _, exists := s.msgs[rcvd_message]; exists {
		s.msgMutex.Unlock()
		return nil
	}
	s.msgs[rcvd_message] = true
	s.msgMutex.Unlock()

	for _, neighbor := range s.neighbors {
		if neighbor != msg.Src {
			go func(neighbor string) {
				if err := s.n.Send(neighbor, map[string]any{
					"type":    "broadcast",
					"message": body["message"],
				}); err != nil {
					panic(err)
				}
			}(neighbor)
		}
	}

	// Inter-server messages don't have a msg_id, so don't need a response
	if _, exists := body["msg_id"]; exists {
		return s.n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	}

	return nil
}

func (s *server) gossip(src string, body map[string]any) error {
	for _, neighbor := range s.neighbors {
		if neighbor != src && neighbor != s.n.ID() {
			go func(neighbor string) {
				if err := s.n.Send(neighbor, map[string]any{
					"type":    "broadcast",
					"message": body["message"],
				}); err != nil {
					panic(err)
				}
			}(neighbor)
		}
	}
	return nil
}
