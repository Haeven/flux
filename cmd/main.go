package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/yourusername/flux/internal/gard"
	"github.com/yourusername/flux/internal/redpanda"
)

const (
	tcpPort = ":8080"
)

func main() {
	// Initialize Redpanda client
	redpandaClient, err := redpanda.NewClient()
	if err != nil {
		log.Fatalf("Failed to create Redpanda client: %v", err)
	}
	defer redpandaClient.Close()

	// Initialize Gard client
	gardClient, err := gard.NewClient()
	if err != nil {
		log.Fatalf("Failed to create Gard client: %v", err)
	}
	defer gardClient.Close()

	// Start TCP server
	listener, err := net.Listen("tcp", tcpPort)
	if err != nil {
		log.Fatalf("Failed to start TCP server: %v", err)
	}
	defer listener.Close()

	fmt.Printf("Flux server listening on %s\n", tcpPort)

	// Handle graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}
			go handleConnection(conn, redpandaClient, gardClient)
		}
	}()

	<-shutdown
	fmt.Println("Shutting down Flux server...")
}
func handleConnection(conn net.Conn, redpandaClient *kgo.Client, gardClient *gard.Client) {
	defer conn.Close()

	// Create buffers for reading and writing
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Start a goroutine to receive messages from Redpanda and send them to the client
	go receiveFromRedpanda(redpandaClient, writer)

	for {
		// 1. Receive messages from the client
		message, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from client: %v", err)
			}
			break
		}
		message = strings.TrimSpace(message)

		// Check for special commands
		if strings.HasPrefix(message, "UPLOAD:") {
			// 4. Handle file uploads from the client to Gard
			fileName := strings.TrimPrefix(message, "UPLOAD:")
			handleFileUpload(reader, gardClient, fileName)
		} else if strings.HasPrefix(message, "DOWNLOAD:") {
			// 5. Handle file downloads from Gard to the client
			fileName := strings.TrimPrefix(message, "DOWNLOAD:")
			handleFileDownload(writer, gardClient, fileName)
		} else {
			// 2. Send messages to Redpanda
			sendToRedpanda(redpandaClient, message)
		}
	}
}

func receiveFromRedpanda(redpandaClient *kgo.Client, writer *bufio.Writer) {
	// 3. Receive messages from Redpanda and send them to the client
	for {
		fetches := redpandaClient.PollFetches(context.Background())
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Printf("Error polling Redpanda: %v", errs)
			continue
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			_, err := writer.WriteString(string(record.Value) + "\n")
			if err != nil {
				log.Printf("Error writing to client: %v", err)
				return
			}
			writer.Flush()
		}
	}
}

func sendToRedpanda(redpandaClient *kgo.Client, message string) {
	record := &kgo.Record{
		Topic: "flux_messages",
		Value: []byte(message),
	}
	results := redpandaClient.ProduceSync(context.Background(), record)
	if err := results.FirstErr(); err != nil {
		log.Printf("Error sending message to Redpanda: %v", err)
	}
}

func handleFileUpload(reader *bufio.Reader, gardClient *gard.Client, fileName string) {
	// Read file content from the client
	fileContent, err := reader.ReadBytes('\n')
	if err != nil {
		log.Printf("Error reading file content: %v", err)
		return
	}

	// Upload file to Gard
	err = gardClient.UploadFile(fileName, fileContent)
	if err != nil {
		log.Printf("Error uploading file to Gard: %v", err)
	}
}

func handleFileDownload(writer *bufio.Writer, gardClient *gard.Client, fileName string) {
	// Download file from Gard
	fileContent, err := gardClient.DownloadFile(fileName)
	if err != nil {
		log.Printf("Error downloading file from Gard: %v", err)
		return
	}

	// Send file content to the client
	_, err = writer.Write(fileContent)
	if err != nil {
		log.Printf("Error sending file to client: %v", err)
		return
	}
	writer.Flush()

	// 6. Send client download speed to Gard
	downloadSpeed := calculateDownloadSpeed(len(fileContent))
	err = gardClient.ReportDownloadSpeed(downloadSpeed)
	if err != nil {
		log.Printf("Error reporting download speed to Gard: %v", err)
	}
}

func calculateDownloadSpeed(fileSize int) float64 {
	// Implement download speed calculation logic
	// This is a placeholder implementation
	return float64(fileSize) / 1024 / 1024 // MB/s
}