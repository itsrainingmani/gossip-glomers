package main

import (
	"encoding/json"
	"log"

	uuid "github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

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

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
