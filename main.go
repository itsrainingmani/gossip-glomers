package main

import (
	"encoding/json"
	"log"

	uuid "github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Topology map[string][]string

type TopoMsg struct {
	Type      string   `json:"type"`
	Topology  Topology `json:"topology"`
	MessageID int      `json:"msg_id"`
}

type BroadcastMsg struct {
	Type      string `json:"type"`
	Message   any    `json:"message"`
	MessageID int    `json:"msg_id"`
}

var topo Topology

func main() {
	n := maelstrom.NewNode()
	received_msgs := make([]any, 10)

	// Challenge #1 : Echo - https://fly.io/dist-sys/1/
	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "echo_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	// Challenge #2: Unique ID Generation - https://fly.io/dist-sys/2/
	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msg_id := body["msg_id"]
		body["type"] = "generate_ok"
		body["in_reply_to"] = msg_id

		// Since the ID may be of any-type, we use the uuid package to generate IDs
		body["id"] = uuid.New()

		return n.Reply(msg, body)
	})

	// Challenge #3: Broadcast - https://fly.io/dist-sys/3a/
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body TopoMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topo = body.Topology

		ret := map[string]any{}

		ret["type"] = "topology_ok"
		ret["in_reply_to"] = body.MessageID
		return n.Reply(msg, ret)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		ret := map[string]any{}
		ret["type"] = "read_ok"
		ret["in_reply_to"] = body["msg_id"]
		ret["messages"] = received_msgs
		return n.Reply(msg, ret)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if body.Message != nil {
			received_msgs = append(received_msgs, body.Message)
		}

		// Use the network topology to send a message to all nodes
		// make queue of neighboring nodes
		// visited := set.New[string](10)
		// queue := make([]string, 10)

		if _, ok := topo[msg.Src]; !ok {
			for _, elem := range n.NodeIDs() {
				if elem != n.ID() {
					n.Send(elem, body)
				}
			}
		}

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
