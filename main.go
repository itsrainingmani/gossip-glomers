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

type Server struct {
	Node      *maelstrom.Node
	Neighbors []string
	Msgs      map[int]bool
	NextMsgID int
	Callbacks map[int]*maelstrom.HandlerFunc

	Mutex sync.RWMutex
}

func main() {
	node := maelstrom.NewNode()
	server := &Server{Node: node, Msgs: make(map[int]bool), NextMsgID: 0, Callbacks: make(map[int]*maelstrom.HandlerFunc)}

	node.Handle("echo", server.echoHandler)
	node.Handle("generate", server.generateHandler)
	node.Handle("broadcast", server.broadcastHandler)
	node.Handle("read", server.readHandler)
	node.Handle("topology", server.topologyHandler)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

// Challenge #1 : Echo - https://fly.io/dist-sys/1/
func (s *Server) echoHandler(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Update the message type to return back.
	body["type"] = "echo_ok"

	// Echo the original message back with the updated message type.
	return s.Node.Reply(msg, body)
}

// Challenge #2: Unique ID Generation - https://fly.io/dist-sys/2/
func (s *Server) generateHandler(msg maelstrom.Message) error {
	// Since the ID may be of any-type, we use the uuid package to generate IDs
	return s.Node.Reply(msg, map[string]any{
		"type": "generate_ok",
		"id":   uuid.New(),
	})
}

// Challenge #3: Broadcast - https://fly.io/dist-sys/3a/
func (s *Server) topologyHandler(msg maelstrom.Message) error {
	var body TopoMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.Neighbors = body.Topology[s.Node.ID()]
	return s.Node.Reply(msg, map[string]any{
		"type":        "topology_ok",
		"in_reply_to": body.MessageID,
	})
}

func (s *Server) readHandler(msg maelstrom.Message) error {
	// Don't really need to handle the message body because we're only going to send back all the messages that we've received
	s.Mutex.RLock()
	messages := make([]int, 0, len(s.Msgs))
	for id := range s.Msgs {
		messages = append(messages, id)
	}
	s.Mutex.RUnlock()
	return s.Node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}

func (s *Server) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	rcvd_message := int(body["message"].(float64))

	s.Mutex.Lock()
	if _, exists := s.Msgs[rcvd_message]; exists {
		s.Mutex.Unlock()
		return nil
	}
	s.Msgs[rcvd_message] = true
	s.Mutex.Unlock()

	for _, neighbor := range s.Neighbors {
		if neighbor != msg.Src {
			go func(neighbor string) {
				if err := s.Node.Send(neighbor, map[string]any{
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
		return s.Node.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	}

	return nil
}

func (s *Server) rpc(dest string, body map[string]any, handler *maelstrom.HandlerFunc) error {
	s.Mutex.Lock()
	s.NextMsgID += 1
	msg_id := s.NextMsgID

	s.Callbacks[msg_id] = handler
	body["msg_id"] = msg_id

	if err := s.Node.Send(dest, body); err != nil {
		panic(err)
	}

	s.Mutex.Unlock()
	return nil
}
