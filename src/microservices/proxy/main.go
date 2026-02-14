package main

import (
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
)

var (
	monolithURL   string
	moviesURL     string
	eventsURL     string
	gradualMig    bool
	migrationPct  int
	client        = &http.Client{}
)

func main() {
	monolithURL = strings.TrimSuffix(getEnv("MONOLITH_URL", "http://monolith:8080"), "/")
	moviesURL = strings.TrimSuffix(getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"), "/")
	eventsURL = strings.TrimSuffix(getEnv("EVENTS_SERVICE_URL", "http://events-service:8082"), "/")
	gradualMig = getEnv("GRADUAL_MIGRATION", "true") == "true"
	migrationPct, _ = strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "50"))
	if migrationPct < 0 {
		migrationPct = 0
	}
	if migrationPct > 100 {
		migrationPct = 100
	}

	http.HandleFunc("/health", health)
	http.HandleFunc("/api/", apiHandler)

	port := getEnv("PORT", "8000")
	log.Printf("proxy listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Strangler Fig Proxy is healthy"))
}

func apiHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	var targetBase string
	switch {
	case strings.HasPrefix(path, "/api/events"):
		targetBase = eventsURL
	case strings.HasPrefix(path, "/api/movies"):
		targetBase = moviesTarget()
	case strings.HasPrefix(path, "/api/users"), strings.HasPrefix(path, "/api/payments"), strings.HasPrefix(path, "/api/subscriptions"):
		targetBase = monolithURL
	default:
		targetBase = monolithURL
	}
	target := targetBase + path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	proxyTo(w, r, target)
}

func moviesTarget() string {
	if !gradualMig {
		return monolithURL
	}
	if rand.Intn(100) < migrationPct {
		return moviesURL
	}
	return monolithURL
}

func proxyTo(w http.ResponseWriter, r *http.Request, target string) {
	req, err := http.NewRequest(r.Method, target, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	for k, v := range r.Header {
		if strings.EqualFold(k, "Host") {
			continue
		}
		req.Header[k] = v
	}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
