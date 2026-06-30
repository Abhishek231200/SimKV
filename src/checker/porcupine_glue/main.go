// Porcupine linearizability checker for SimKV history JSON.
// Reads the JSON produced by simkv (History::to_json) and checks each key
// independently using Porcupine's sequential consistency model.
//
// Usage: simkv_porcupine <history.json>
// Exit 0: linearizable. Exit 1: not linearizable or error.

package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/anishathalye/porcupine"
)

// ─── JSON schema (matches History::to_json output) ──────────────────────────

type HistEntry struct {
	ID         int    `json:"id"`
	ClientID   uint64 `json:"client_id"`
	RequestID  uint64 `json:"request_id"`
	Op         string `json:"op"`
	Key        string `json:"key"`
	Value      string `json:"value"`
	Expected   string `json:"expected"`
	Start      int64  `json:"start"`
	End        int64  `json:"end"`
	OK         bool   `json:"ok"`
	Response   string `json:"response"`
}

// ─── KV state-machine model for Porcupine ───────────────────────────────────

type kvInput struct {
	op       string
	key      string
	value    string
	expected string
}

type kvOutput struct {
	ok       bool
	response string
}

// kvState is the per-key register value (empty string = absent).
type kvState = string

var kvModel = porcupine.Model{
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		s := state.(string)
		in := input.(kvInput)
		out := output.(kvOutput)

		switch in.op {
		case "put":
			return out.ok, in.value
		case "get":
			return s == out.response, s
		case "cas":
			if s == in.expected {
				return out.ok, in.value
			}
			return !out.ok, s
		}
		return false, s
	},
	Equal: func(s1, s2 interface{}) bool { return s1.(string) == s2.(string) },
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: simkv_porcupine <history.json>")
		os.Exit(1)
	}

	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading file: %v\n", err)
		os.Exit(1)
	}

	var entries []HistEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing JSON: %v\n", err)
		os.Exit(1)
	}

	// Decompose by key.
	byKey := make(map[string][]porcupine.Operation)
	for _, e := range entries {
		if e.Op == "" {
			continue
		}
		op := porcupine.Operation{
			ClientId: int(e.ClientID),
			Input: kvInput{
				op:       e.Op,
				key:      e.Key,
				value:    e.Value,
				expected: e.Expected,
			},
			Output: kvOutput{
				ok:       e.OK,
				response: e.Response,
			},
			Call:    e.Start,
			Return:  e.End,
		}
		byKey[e.Key] = append(byKey[e.Key], op)
	}

	allOK := true
	for key, ops := range byKey {
		result := porcupine.CheckOperations(kvModel, ops)
		if result != porcupine.Ok {
			fmt.Printf("FAIL: key=%q is not linearizable\n", key)
			allOK = false
		}
	}

	if allOK {
		fmt.Printf("PASS: all %d keys are linearizable (%d operations)\n",
			len(byKey), len(entries))
		os.Exit(0)
	} else {
		os.Exit(1)
	}
}
