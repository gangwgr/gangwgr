package main

import (
	"fmt"
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
	// Example Inputs
	object1 := map[string]interface{}{
		"a": map[string]interface{}{
			"b": map[string]interface{}{
				"c": "d",
			},
		},
	}

	object2 := map[string]interface{}{
		"x": map[string]interface{}{
			"y": map[string]interface{}{
				"z": "a",
			},
		},
	}

	key1 := "a/b/c"
	key2 := "x/y/z"

	value1 := getNestedValue(object1, key1)
	value2 := getNestedValue(object2, key2)

	fmt.Println("Value 1:", value1) // Output: Value 1: d
	fmt.Println("Value 2:", value2) // Output: Value 2: a
}
