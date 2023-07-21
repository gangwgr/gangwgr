package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func getNestedValue(obj map[string]interface{}, key string) interface{} {
	keys := strings.Split(key, "/")

	for _, k := range keys {
		value, found := obj[k]
		if !found {
			return nil
		}

		// If the current value is not a nested object, return it
		if val, ok := value.(map[string]interface{}); ok {
			obj = val
		} else {
			return value
		}
	}

	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <JSON object> <key>")
		return
	}

	// Parse the JSON object provided as a string argument
	var object map[string]interface{}
	if err := json.Unmarshal([]byte(os.Args[1]), &object); err != nil {
		fmt.Println("Error parsing JSON object:", err)
		return
	}

	key := os.Args[2]

	value := getNestedValue(object, key)

	fmt.Println("Value:", value)
}
