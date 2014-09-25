package client

import (
	"keyvalue/server"

	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"bytes"
	"math/rand"
)

func TestClient(t *testing.T) {
	os.RemoveAll(server.LogDir)
	server.Init(12345)
	c := clientInit("localhost:12345")
	//c := clientInit("adelie-01:12345")
	sanityTest(c)
	correctnessTest(c)
	performanceTest(c, 100)
	performanceTest(c, 200)
	performanceTest(c, 500)
	performanceTest(c, 1000)
	performanceTest(c, 5000)
}

func clientInit(server string) *Client {
	status, client := Init(server)

	if status != 0 {
		log.Fatal("Client inited with nonzero status")
	}
	if client == nil {
		log.Fatal("Client inited returned nil value")
	}

	log.Printf("Successfully connected to Server at, %s", server)
	return client
}

func sanityTest(client *Client) {
	printTestStart("Sanity Test")
	result, old := client.Set("key1", "value1")
	log.Printf("Called Set(key=%s, value=%s) Received(result=%d, value=%s)\n", "key1", "value1", result, old)
	result, value := client.Get("key1")
	log.Printf("Called Get(key=%s) Received(result=%d, value=%s)\n", "key1", result, value)
}

func correctnessTest(client *Client) {
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

func performanceTest(client *Client, valueSize int64) {
	printTestStart("Performance Test")

	var buffer bytes.Buffer
	var i int64

	for i = 0; i < valueSize; i++ {
		buffer.WriteString("a")
	}

	value := buffer.String()
	log.Printf("Value Size: %d bytes", valueSize)
	operations := 10000

	startTime := time.Now()
	total, min, max := seqWrite(client, operations, value)
	elapsed := time.Since(startTime)
	log.Printf("Write test - Keys: %d, Total time: %s, %f ops/sec, Latency: Avg %f, Min %f, Max %f s", operations, elapsed, float64(operations)/elapsed.Seconds(), total/float64(operations), min, max)

	startTime = time.Now()
	total, min, max = seqRead(client, operations, value)
	elapsed = time.Since(startTime)
	log.Printf("SeqRead test - Keys: %d, Total time: %s, %f ops/sec, Latency: Avg %f, Min %f, Max %f s", operations, elapsed, float64(operations)/elapsed.Seconds(), total/float64(operations), min, max)

	startTime = time.Now()
	total, min, max = randRead(client, operations, value)
	elapsed = time.Since(startTime)
	log.Printf("RandRead test - Keys: %d, Total time: %s, %f ops/sec, Latency: Avg %f, Min %f, Max %f s", operations, elapsed, float64(operations)/elapsed.Seconds(), total/float64(operations), min, max)

	log.Printf("PASS")
}

func seqWrite(client *Client, numKeys int, value string) (float64, float64, float64) {
	var latency float64
	totalLatency := 0.0
	maxLatency := 0.0
	minLatency := 0.0
	var start time.Time
	var elapsed time.Duration

	for i := 1; i < numKeys; i++ {
		key := strconv.Itoa(i)
		start = time.Now()
		result, _ := client.Set(key, value)
		elapsed = time.Since(start)
		latency = elapsed.Seconds()
		if latency > maxLatency {
			maxLatency = latency
		}
		if minLatency == 0.0 {
			minLatency = latency
		} else if latency < minLatency {
			minLatency = latency
		}
		totalLatency += latency

		if result == -1 {
			log.Fatalf("Write failure. Failed to write key: %s", key)
		}
	}

	return totalLatency, minLatency, maxLatency
}

func seqRead(client *Client, numKeys int, value string) (float64, float64, float64) {
	var latency float64
	totalLatency := 0.0
	maxLatency := 0.0
	minLatency := 0.0
	var start time.Time
	var elapsed time.Duration

	for i := 1; i < numKeys; i++ {
		var n int
		for n = rand.Intn(numKeys); n == 0; n = rand.Intn(numKeys) {
		}
		key := strconv.Itoa(n)
		start = time.Now()
		result, out := client.Get(key)
		elapsed = time.Since(start)
		latency = elapsed.Seconds()
		if latency > maxLatency {
			maxLatency = latency
		}
		if minLatency == 0.0 {
			minLatency = latency
		} else if latency < minLatency {
			minLatency = latency
		}
		totalLatency += latency

		if result == -1 {
			log.Fatalf("Read failure. Failed to read key: %s", key)
		}

		if out != value {
			log.Fatalf("Inconsistent data on read. Result: %d, Expecting: %s, Received: %s", result, value, out)
		}
	}

	return totalLatency, minLatency, maxLatency
}

func randRead(client *Client, numKeys int, value string) (float64, float64, float64) {
	var latency float64
	totalLatency := 0.0
	maxLatency := 0.0
	minLatency := 0.0
	var start time.Time
	var elapsed time.Duration

	for i := 1; i < numKeys; i++ {
		key := strconv.Itoa(i)
		start = time.Now()
		result, out := client.Get(key)
		elapsed = time.Since(start)
		latency = elapsed.Seconds()
		if latency > maxLatency {
			maxLatency = latency
		}
		if minLatency == 0.0 {
			minLatency = latency
		} else if latency < minLatency {
			minLatency = latency
		}
		totalLatency += latency

		if result == -1 {
			log.Fatalf("Read failure. Failed to read key: %s", key)
		}

		if out != value {
			log.Fatalf("Inconsistent data on read. Result: %d, Expecting: %s, Received: %s", result, value, out)
		}
	}

	return totalLatency, minLatency, maxLatency
}

func printTestStart(testName string) {
	log.Printf("----------------- %s ------------------", testName)
}
