package main

import "net/http"

// openAPISpec is hand-written rather than reflection-generated: this API's
// surface is small enough that a generator would add a dependency without
// saving meaningful effort, and a hand-written spec stays exactly in sync
// with what's documented as intentional (vs. accidentally exposing a field
// a generator picked up from a struct).
const openAPISpec = `{
  "openapi": "3.0.3",
  "info": {
    "title": "InduSense API",
    "version": "1.0.0",
    "description": "Industrial IoT monitoring platform API — factories, devices, telemetry, alerts, and incidents."
  },
  "servers": [{"url": "/api/v1"}],
  "components": {
    "securitySchemes": {
      "bearerAuth": {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}
    },
    "schemas": {
      "Error": {
        "type": "object",
        "properties": {
          "error": {
            "type": "object",
            "properties": {
              "code": {"type": "string"},
              "message": {"type": "string"},
              "request_id": {"type": "string"}
            }
          }
        }
      }
    }
  },
  "security": [{"bearerAuth": []}],
  "paths": {
    "/auth/login": {
      "post": {
        "summary": "Log in with email and password",
        "security": [],
        "requestBody": {"required": true, "content": {"application/json": {"schema": {"type": "object", "properties": {"email": {"type": "string"}, "password": {"type": "string"}}}}}},
        "responses": {"200": {"description": "Access and refresh tokens issued"}, "401": {"description": "Invalid credentials", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}}}
      }
    },
    "/auth/refresh": {
      "post": {"summary": "Exchange a refresh token for a new token pair (rotates the refresh token)", "security": [], "responses": {"200": {"description": "New token pair"}, "401": {"description": "Refresh token revoked or invalid"}}}
    },
    "/auth/logout": {
      "post": {"summary": "Revoke a refresh token", "security": [], "responses": {"204": {"description": "Logged out"}}}
    },
    "/auth/me": {
      "get": {"summary": "Current user's identity and permissions (from the access token)", "responses": {"200": {"description": "User info"}}}
    },
    "/factories": {
      "get": {"summary": "List factories in the caller's organization", "parameters": [{"name": "limit", "in": "query", "schema": {"type": "integer"}}, {"name": "offset", "in": "query", "schema": {"type": "integer"}}], "responses": {"200": {"description": "Paginated factory list"}}}
    },
    "/factories/{id}": {
      "get": {"summary": "Get one factory", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"200": {"description": "Factory"}, "404": {"description": "Not found"}}}
    },
    "/factories/{id}/production-lines": {
      "get": {"summary": "List a factory's production lines", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"200": {"description": "Production lines"}}}
    },
    "/production-lines/{id}/machines": {
      "get": {"summary": "List a production line's machines", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"200": {"description": "Machines"}}}
    },
    "/machines/{id}": {
      "get": {"summary": "Get one machine", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"200": {"description": "Machine"}, "404": {"description": "Not found"}}}
    },
    "/machines/{id}/devices": {
      "get": {"summary": "List a machine's devices", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"200": {"description": "Devices"}}}
    },
    "/devices": {
      "get": {"summary": "List devices, optionally filtered by status", "parameters": [{"name": "status", "in": "query", "schema": {"type": "string"}}], "responses": {"200": {"description": "Paginated device list"}}},
      "post": {"summary": "Provision a new device (returns its secret exactly once)", "requestBody": {"required": true}, "responses": {"201": {"description": "Device provisioned"}}}
    },
    "/devices/{id}": {
      "get": {"summary": "Get one device", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"200": {"description": "Device"}, "404": {"description": "Not found"}}}
    },
    "/devices/{id}/sensors": {
      "get": {"summary": "List a device's sensors", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"200": {"description": "Sensors"}}}
    },
    "/devices/{id}/rotate-credentials": {
      "post": {"summary": "Deactivate the device's current credential and issue a new one", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"200": {"description": "New secret (shown once)"}}}
    },
    "/devices/{id}/decommission": {
      "post": {"summary": "Mark a device decommissioned", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"204": {"description": "Decommissioned"}}}
    },
    "/telemetry/latest": {
      "get": {"summary": "Most recent reading for a device+metric", "parameters": [{"name": "device_id", "in": "query", "required": true, "schema": {"type": "string"}}, {"name": "metric", "in": "query", "required": true, "schema": {"type": "string"}}], "responses": {"200": {"description": "Latest point"}, "404": {"description": "No data"}}}
    },
    "/telemetry/range": {
      "get": {"summary": "Readings over a time range: range=5m|1h|24h, or start (and optionally end) as RFC3339", "parameters": [{"name": "device_id", "in": "query", "required": true, "schema": {"type": "string"}}, {"name": "metric", "in": "query", "required": true, "schema": {"type": "string"}}, {"name": "range", "in": "query", "schema": {"type": "string"}}, {"name": "start", "in": "query", "schema": {"type": "string"}}, {"name": "end", "in": "query", "schema": {"type": "string"}}], "responses": {"200": {"description": "Time series"}}}
    },
    "/alerts": {
      "get": {"summary": "List alerts", "parameters": [{"name": "status", "in": "query", "schema": {"type": "string"}}, {"name": "severity", "in": "query", "schema": {"type": "string"}}], "responses": {"200": {"description": "Paginated alert list"}}}
    },
    "/alerts/{id}": {
      "get": {"summary": "Get one alert", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"200": {"description": "Alert"}, "404": {"description": "Not found"}}}
    },
    "/alerts/{id}/acknowledge": {
      "post": {"summary": "Acknowledge an OPEN alert", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"204": {"description": "Acknowledged"}, "409": {"description": "Not OPEN or not found"}}}
    },
    "/incidents": {
      "get": {"summary": "List incidents", "parameters": [{"name": "status", "in": "query", "schema": {"type": "string"}}], "responses": {"200": {"description": "Paginated incident list"}}}
    },
    "/incidents/{id}": {
      "get": {"summary": "Get one incident with its full audit history", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"200": {"description": "Incident + history"}, "404": {"description": "Not found"}}}
    },
    "/incidents/{id}/transition": {
      "post": {"summary": "Move an incident to a new status (OPEN/ACKNOWLEDGED/INVESTIGATING/RESOLVED/CLOSED)", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"204": {"description": "Transitioned"}, "409": {"description": "Invalid transition"}}}
    },
    "/incidents/{id}/assign": {
      "post": {"summary": "Assign an incident to a technician", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"204": {"description": "Assigned"}}}
    },
    "/incidents/{id}/resolve": {
      "post": {"summary": "Resolve an incident with resolution notes", "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}], "responses": {"204": {"description": "Resolved"}, "409": {"description": "Invalid transition"}}}
    }
  }
}`

func handleOpenAPISpec() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAPISpec))
	}
}

// handleSwaggerUI serves a minimal Swagger UI page loading the spec above.
// The Swagger UI bundle itself is loaded from a CDN (unpkg) — acceptable
// for a local development docs page, not part of the running application's
// production request path.
func handleSwaggerUI() http.HandlerFunc {
	const page = `<!DOCTYPE html>
<html>
<head>
  <title>InduSense API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => SwaggerUIBundle({ url: '/api/v1/openapi.json', dom_id: '#swagger-ui' });
  </script>
</body>
</html>`
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(page))
	}
}
