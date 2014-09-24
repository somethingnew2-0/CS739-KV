package main

import (
	"keyvalue/client"
	"keyvalue/server"

	"log"
	"runtime"
	"strconv"
	"time"
)

func init() {
	// Set runtime GOMAXPROCS
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	server.Init(12345)
	c := clientInit("localhost:12345")
	sanityTest(c)
	correctnessTest(c)
	performanceTest(c)

}

func clientInit(server string) *client.Client {
	status, client := client.Init(server)

	if status != 0 {
		log.Fatal("Client inited with nonzero status")
	}
	if client == nil {
		log.Fatal("Client inited returned nil value")
	}

	log.Printf("Successfully connected to Server at, %s", server)
	return client
}

func sanityTest(client *client.Client) {
	printTestStart("Sanity Test")

	result, old := client.Set("key1", "value1")

	log.Printf("Called Set(key=%s, value=%s) Received(result=%d, value=%s)\n", "key1", "value1", result, old)

	result, value := client.Get("key1")

	log.Printf("Called Get(key=%s) Received(result=%d, value=%s)\n", "key1", result, value)
}

func correctnessTest(client *client.Client) {
	printTestStart("Correctness Test")

	var value string = "This is a sample test value of type string"
	var result int
	var out string

	// Test Case 1: Write a new key
	result, out = client.Set("New_key_1", value)
	if result != 1 {
		log.Fatal("TC 1: Server did not return status 1 for writing a new key. Received : ", result)
	}
	if out != "" {
		log.Fatal("TC 1: Server returned old value for writing a new key. Received value : ", out)
	}

	// Test Case 2: Overwrite an existing key
	var new_value string = "A different sample string value"
	result, out = client.Set("New_key_1", new_value)
	if result != 0 {
		log.Fatal("TC 2: Server did not return status 0 for writing to an existing key. Received : ", result)
	}
	if out != value {
		log.Fatalf("TC 2: Server did not return the expected old value for writing to an existing key. Expecting: %s, Received: %s ", value, out)
	}

	// Test Case 3: Read an existing key
	result, out = client.Get("New_key_1")
	if result != 0 {
		log.Fatal("TC 3: Server did not return status 0 for reading an existing key. Received : ", result)
	}
	if out != new_value {
		log.Fatalf("TC 3: Server did not return the expected value for reading an existing key. Expecting: %s, Received: %s ", new_value, out)
	}

	// Test Case 4: Read a non-existent key
	result, out = client.Get("Madeup_key")
	if result != 1 {
		log.Fatal("TC 4: Server did not return status 1 for reading a non-existent key. Received : ", result)
	}
	if out != "" {
		log.Fatalf("TC 4: Server returned a value for a non-existent key. Received: %s", out)
	}

	log.Printf("PASS")

}

func performanceTest(client *client.Client) {
	printTestStart("Performance Test")

	var value string = "This is a sample test value of type string"
	var elapsed time.Duration
	var startTime time.Time

	startTime = time.Now()
	seqWrite(client, 1000, value)
	elapsed = time.Since(startTime)
	log.Printf("Write test - Keys: %d, Total time: %s", 1000, elapsed)

	startTime = time.Now()
	seqRead(client, 1000, value)
	elapsed = time.Since(startTime)
	log.Printf("SeqRead test - Keys: %d, Total time: %s", 1000, elapsed)

	log.Printf("PASS")
}

func seqWrite(client *client.Client, numKeys int, value string) {
	for i := 1; i < numKeys; i++ {
		key := strconv.Itoa(i)
		result, _ := client.Set(key, value)

		if result == -1 {
			log.Fatalf("Write failure. Failed to write key: %s", key)
		}
	}
}

func seqRead(client *client.Client, numKeys int, value string) {
	for i := 1; i < numKeys; i++ {
		key := strconv.Itoa(i)
		result, out := client.Get(key)

		if result == -1 {
			log.Fatalf("Read failure. Failed to read key: %s", key)
		}

		if out != value {
			log.Fatalf("Inconsistent data on read. Result: %d, Expecting: %s, Received: %s", result, value, out)
		}
	}
}

func printTestStart(testName string) {
	log.Printf("----------------- %s ------------------", testName)
}
