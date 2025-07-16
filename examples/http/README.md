# HTTP Server Integration Examples

This example shows practical examples of integrating daramjwee cache with HTTP servers. It explains how to utilize cache in various types of HTTP servers.

## Included Server Examples

1. **Web Proxy Server**
   - Proxy server that caches external URLs
   - ETag-based conditional request handling
   - Hybrid cache configuration (memory + disk)
   - Port: 8081

2. **API Caching Server**
   - Server that caches API responses
   - Database query delay simulation
   - Memory cache using SIEVE policy
   - Port: 8082

3. **Static File Server**
   - Server that caches static files
   - Content-Type setting based on file type
   - File-based cache store
   - Port: 8083

4. **Database Caching Server**
   - Server that caches complex DB query results
   - Hybrid cache configuration (memory + disk)
   - Cache status monitoring endpoint
   - Port: 8084

## How to Run

```bash
go run examples/http/main.go
```

Once the servers start, visit these URLs:
- http://localhost:8081/proxy?url=https://httpbin.org/json
- http://localhost:8082/api/users
- http://localhost:8083/static/example.txt
- http://localhost:8084/db/users

## Implemented Fetcher Interfaces

- `HTTPFetcher`: Fetcher that retrieves data through HTTP requests
- `DatabaseFetcher`: Fetcher that simulates database queries
- `StaticFileFetcher`: Fetcher that simulates static files