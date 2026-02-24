package statemachine

import (
	"fmt"
	"sort"
	"strings"
)

func (kv *KVStore) DumpState() string {
	snapshot := kv.Snapshot()
	
	if len(snapshot) == 0 {
		return "(empty)"
	}

	keys := make([]string, 0, len(snapshot))
	for k := range snapshot {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var builder strings.Builder
	for i, k := range keys {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("%s=%s", k, snapshot[k]))
	}
	return builder.String()
}