package statemachine

import (
	"encoding/json"
	"fmt"
)
type CommandType string;

const (
	CommandPut CommandType = "PUT"
	CommandDelete CommandType = "DELETE"
)

type Command struct {
	Type CommandType `json:"type"`
	Key string `json:"key"`
	Value string `json:"value"`
}

func (c Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}

func NewPutCommand(key, value string) Command {
	return Command{
		Type:  CommandPut,
		Key:   key,
		Value: value,
	}
}

func NewDeleteCommand(key string) Command {
	return Command{
		Type: CommandDelete,
		Key:  key,
	}
}

func (c Command) String() string {
	switch c.Type {
	case CommandPut:
		return fmt.Sprintf("PUT(%s=%s)", c.Key, c.Value)
	case CommandDelete:
		return fmt.Sprintf("DELETE(%s)", c.Key)
	default:
		return fmt.Sprintf("UNKNOWN(%v)", c)
	}
}