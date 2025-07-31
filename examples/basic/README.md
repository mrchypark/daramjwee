# Basic HTTP Server Example

This example demonstrates the basic usage of daramjwee cache. It implements a simple HTTP server using a file-based store and explains how to handle ETag-based conditional requests.

## Key Features

- File-based hot store configuration
- Virtual origin server simulation
- ETag-based conditional request handling
- Cached content serving through HTTP server

## How to Run

```bash
go run examples/basic/main.go
```

Once the server starts, visit these URLs:
- http://localhost:8080/objects/hello
- http://localhost:8080/objects/world
- http://localhost:8080/objects/not-exist

## Code Explanation

- `fakeOrigin`: Map simulating a virtual origin server
- `originFetcher`: Implementation of daramjwee.Fetcher interface
- HTTP handler: Processes requests and serves cached content