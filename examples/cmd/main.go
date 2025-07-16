// Package main provides a unified entry point for all daramjwee examples
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		return
	}

	switch os.Args[1] {
	case "basic":
		runExample("basic")
	case "scenarios":
		runExample("scenarios")
	case "advanced":
		runExample("advanced")
	case "http":
		runExample("http")
	case "config":
		runExample("config")
	case "all":
		runAllExamples()
	default:
		printUsage()
	}
}

func printUsage() {
	fmt.Println("daramjwee Example Runner")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  go run examples/cmd/main.go <command>")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  basic      - Run basic HTTP server example")
	fmt.Println("  scenarios  - Run scenario-based configuration examples")
	fmt.Println("  advanced   - Run advanced features and performance test examples")
	fmt.Println("  http       - Run HTTP server integration examples")
	fmt.Println("  config     - Run comprehensive configuration examples")
	fmt.Println("  all        - Run all examples (excluding servers)")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  go run examples/cmd/main.go basic")
	fmt.Println("  go run examples/cmd/main.go scenarios")
	fmt.Println()
	fmt.Println("Run individual examples directly:")
	fmt.Println("  go run examples/basic/main.go")
	fmt.Println("  go run examples/scenarios/main.go")
	fmt.Println("  go run examples/advanced/main.go")
	fmt.Println("  go run examples/http/main.go")
	fmt.Println("  go run examples/config/main.go")
}

func runExample(exampleType string) {
	fmt.Printf("=== Running %s example ===\n", exampleType)

	// Get current directory
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Failed to get current directory: %v\n", err)
		os.Exit(1)
	}

	// Construct example path
	examplePath := filepath.Join(currentDir, "examples", exampleType, "main.go")

	// Check if path exists
	if _, err := os.Stat(examplePath); os.IsNotExist(err) {
		fmt.Printf("Example file does not exist: %s\n", examplePath)
		os.Exit(1)
	}

	cmd := exec.Command("go", "run", examplePath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		fmt.Printf("Failed to run example: %v\n", err)
		os.Exit(1)
	}
}

func runAllExamples() {
	fmt.Println("=== Running all daramjwee examples ===")

	examples := []string{"scenarios", "advanced", "config"}

	for i, example := range examples {
		if i > 0 {
			fmt.Println("\n" + strings.Repeat("=", 50))
		}
		runExample(example)
	}

	fmt.Println("\n=== All examples completed ===")
}
