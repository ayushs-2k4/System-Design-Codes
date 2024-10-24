package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net/http"
	"os"
	"slices"
	"sort"
)

type StorageNode struct {
	name string
	host string
}

func (s *StorageNode) fetchFile(path string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("https://{%s}:1231/{%s}", s.host, path))

	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)

	if err != nil {
		return "", err
	}

	return string(body), nil
}

func (s *StorageNode) putFile(path string) (string, error) {
	content, err := os.ReadFile(path)

	if err != nil {
		return "", err
	}

	// Make a POST request to upload the file content
	resp, err := http.Post(fmt.Sprintf("https://%s:1231/%s", s.host, path), "text/plain", bytes.NewBuffer(content))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// Read the response body
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(responseBody), nil
}

type ConsistentHashing struct {
	totalSlots   int64
	currentSlots int64
	keys         []int64
	nodes        []StorageNode
}

func NewConsistentHashing(totalNodes int64) *ConsistentHashing {
	return &ConsistentHashing{
		totalSlots:   totalNodes,
		currentSlots: 0,
	}
}

func (c *ConsistentHashing) hashFun(key string) int64 {
	h := sha256.New()
	h.Write([]byte(key))

	a := new(big.Int).SetBytes(h.Sum(nil))
	b := new(big.Int).SetInt64(c.totalSlots)

	result := new(big.Int).Mod(a, b).Int64()

	return result
}

func (c *ConsistentHashing) addNode(node StorageNode) {
	if c.currentSlots == c.totalSlots {
		log.Fatalf("Can not increase number of nodes")
	}

	key := c.hashFun(node.host)

	index := bisect(c.keys, key)

	// Collision check
	if c.keys[index] == key {
		log.Fatalf("Collision occured")
	}

	c.keys = slices.Insert(c.keys, index, key)
	c.nodes = slices.Insert(c.nodes, index, node)
}

// Removes the node and returns the key from the hash space on which node was placed
func (c *ConsistentHashing) removeNode(node StorageNode) int64 {
	if len(c.keys) == 0 {
		log.Fatal("Hash space is empty")
	}

	key := c.hashFun(node.host)

	index := bisect(c.keys, key)

	if index >= len(c.keys) || c.keys[index] != key {
		log.Fatalf("Key is invalid")
	}

	slices.Delete(c.keys, index, index)
	slices.Delete(c.nodes, index, index)

	return key
}

// Given an item, the function returns the node it is associated with.
func (c *ConsistentHashing) assign(item string) StorageNode {
	key := c.hashFun(item)

	index := bisect(c.keys, key) % int(c.currentSlots)

	return c.nodes[index]
}

func (c *ConsistentHashing) upload(path string) (string, error) {
	key := c.hashFun(path)

	index := bisect(c.keys, key) % int(c.currentSlots)

	node := c.nodes[index]

	return node.putFile(path)
}

func (c *ConsistentHashing) fetch(path string) (string, error) {
	key := c.hashFun(path)

	index := bisect(c.keys, key) % int(c.currentSlots)

	node := c.nodes[index]

	return node.fetchFile(path)
}

func insertSorted(s []int64, ele int64) []int64 {
	s = append(s, ele)
	i := sort.Search(len(s), func(i int) bool { return s[i] > ele })
	copy(s[i+1:], s[i:])
	s[i] = ele
	return s
}

func bisect(keys []int64, key int64) int {
	return sort.Search(len(keys), func(i int) bool {
		return keys[i] >= key
	})
}

func main() {
	fmt.Println("Hello world!")

	c := NewConsistentHashing(int64(math.Pow(2, 256)))
	res := c.hashFun("Hello world!")

	fmt.Println(res)
}
