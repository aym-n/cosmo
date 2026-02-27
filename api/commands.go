// Package api provides the client-facing contract for encoding KV commands.
// The wire format is shared with the statemachine package (decode/apply).
package api

import "encoding/json"

// CommandType is the type of a KV command (must match statemachine.CommandType values).
type CommandType string

const (
	CommandPut    CommandType = "PUT"
	CommandDelete CommandType = "DELETE"
)

// command is the wire format for a KV command (must match statemachine.Command JSON).
type command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value string      `json:"value"`
}

// EncodePut returns the wire-format bytes for a PUT key=value command.
func EncodePut(key, value string) ([]byte, error) {
	return json.Marshal(command{Type: CommandPut, Key: key, Value: value})
}

// EncodeDelete returns the wire-format bytes for a DELETE key command.
func EncodeDelete(key string) ([]byte, error) {
	return json.Marshal(command{Type: CommandDelete, Key: key})
}
