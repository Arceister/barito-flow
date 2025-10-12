package flow

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zekroTJA/timedmap"
)

func TestEnsureIndexExists_CacheHit(t *testing.T) {
	client := &openSearchClient{
		indexExistsCache: timedmap.New(10 * time.Minute),
	}

	indexName := "test-index-2024.01.01"
	client.indexExistsCache.Set(indexName, true, 10*time.Minute)

	result := client.ensureIndexExists(context.Background(), indexName)

	if !result {
		t.Fatal("expected true when index exists in cache")
	}
}

func TestEnsureIndexExists_IndexExists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/test-index-2024.01.01" && r.Method == "HEAD" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, err := NewOpenSearch(
		NewOpenSearchConfig(1, 1, "test-template"),
		[]string{server.URL},
		"",
		"",
		nil,
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	indexName := "test-index-2024.01.01"
	result := client.ensureIndexExists(context.Background(), indexName)
	fmt.Println(result)

	if !result {
		t.Fatal("expected true when index exists on server")
	}

	cacheValue := client.indexExistsCache.GetValue(indexName)
	if cacheValue == nil {
		t.Fatal("expected index to be cached")
	}
}

func TestEnsureIndexExists_IndexCreated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println(r.Method, r.URL.Path)
		if r.URL.Path == "/test-index-2024.01.01" && r.Method == "HEAD" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Path == "/test-index-2024.01.01" && r.Method == "PUT" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"acknowledged":true}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, err := NewOpenSearch(
		NewOpenSearchConfig(1, 1, "test-template"),
		[]string{server.URL},
		"",
		"",
		nil,
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	indexName := "test-index-2024.01.01"
	result := client.ensureIndexExists(context.Background(), indexName)

	if !result {
		t.Fatal("expected true when index is created")
	}

	cacheValue := client.indexExistsCache.GetValue(indexName)
	if cacheValue == nil {
		t.Fatal("expected index to be cached after creation")
	}
}

func TestEnsureIndexExists_CreateIndexFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/test-index-2024.01.01" && r.Method == "HEAD" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Path == "/test-index-2024.01.01" && r.Method == "PUT" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, err := NewOpenSearch(
		NewOpenSearchConfig(1, 1, "test-template"),
		[]string{server.URL},
		"",
		"",
		nil,
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	indexName := "test-index-2024.01.01"
	result := client.ensureIndexExists(context.Background(), indexName)

	if result {
		t.Fatal("expected false when index creation fails")
	}

	cacheValue := client.indexExistsCache.GetValue(indexName)
	if cacheValue != nil {
		t.Fatal("expected index not to be cached when creation fails")
	}
}

func TestEnsureIndexExists_CheckExistenceFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, err := NewOpenSearch(
		NewOpenSearchConfig(1, 1, "test-template"),
		[]string{server.URL},
		"",
		"",
		nil,
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	indexName := "test-index-2024.01.01"
	result := client.ensureIndexExists(context.Background(), indexName)

	if result {
		t.Fatal("expected false when existence check returns unexpected status")
	}
}
