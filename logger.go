package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

func initLogger() {
	// Check if LOG_FILE environment variable is set
	logFile := os.Getenv("LOG_FILE")
	if logFile != "" {
		// Ensure log directory exists
		logDir := filepath.Dir(logFile)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			fmt.Printf("Failed to create log directory: %v\n", err)
			// Fall back to stdout
			return
		}
		
		// Open log file
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("Failed to open log file: %v\n", err)
			// Fall back to stdout
			return
		}
		
		// Write to both file and stdout
		multiWriter := io.MultiWriter(file, os.Stdout)
		log.SetOutput(multiWriter)
	}
	
	// Set flags for timestamp
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}