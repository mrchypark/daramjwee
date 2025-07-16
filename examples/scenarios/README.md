# Scenario-based Configuration Examples

This example shows daramjwee cache configurations tailored to various real-world usage scenarios. Each scenario proposes optimal cache configurations.

## Included Scenarios

1. **Web Proxy Cache**
   - High throughput and fast response times are critical
   - Memory + disk hybrid configuration
   - Short TTL to maintain freshness

2. **API Response Cache**
   - Fast caching of API responses
   - Short TTL and efficient memory usage
   - SIEVE policy for high hit rates

3. **Image/Media Cache**
   - Large file handling
   - Disk-based storage
   - Long TTL (media files change infrequently)

4. **Database Query Cache**
   - Caching complex query results
   - Medium TTL
   - Memory-first with disk backup

5. **CDN Edge Cache**
   - Very high throughput
   - Regional content caching
   - Efficient storage management

6. **Microservice Inter-communication Cache**
   - Service-to-service communication optimization
   - Medium-sized cache
   - Moderate TTL

7. **Development/Test Environment Cache**
   - Configuration for rapid development
   - Small resource usage
   - Short TTL (quick change reflection)

8. **High-performance Read-only Cache**
   - Read performance optimization
   - Long TTL
   - Optimized lock strategy

## How to Run

```bash
go run examples/scenarios/main.go
```