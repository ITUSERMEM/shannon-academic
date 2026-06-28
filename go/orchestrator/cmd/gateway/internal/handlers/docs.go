package handlers

import (
	_ "embed"
	"net/http"
)

//go:embed swagger.html
var swaggerHTML string

//go:embed openapi.yaml
var openapiYAML string

func DocsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(swaggerHTML))
}

func SpecHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(openapiYAML))
}
