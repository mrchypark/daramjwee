package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/cache"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// User represents a user in our system
type User struct {
	ID       int       `json:"id"`
	Name     string    `json:"name"`
	Email    string    `json:"email"`
	LastSeen time.Time `json:"last_seen"`
}

// Config represents application configuration
type Config struct {
	AppName     string `json:"app_name"`
	Debug       bool   `json:"debug"`
	Port        int    `json:"port"`
	MaxUsers    int    `json:"max_users"`
	DatabaseURL string `json:"database_url"`
}

// Product represents a product in an e-commerce system
type Product struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Price       float64 `json:"price"`
	Category    string  `json:"category"`
	InStock     bool    `json:"in_stock"`
	Description string  `json:"description"`
}

func main() {
	ctx := context.Background()

	// Create temporary directory for file store
	baseDir, err := os.MkdirTemp("", "daramjwee-generic-example-")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(baseDir)

	// Setup logger
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	// Create stores
	memStore := memstore.New(1*1024*1024, policy.NewLRU()) // 1MB memory cache
	fileStore, err := filestore.New(baseDir, logger)
	if err != nil {
		logger.Log("msg", "Failed to create filestore", "err", err)
		os.Exit(1)
	}

	// Create the underlying daramjwee cache with hot/cold stores
	baseCache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(memStore),   // Memory as hot store
		daramjwee.WithColdStore(fileStore), // File as cold store
		daramjwee.WithDefaultTimeout(10*time.Second),
		daramjwee.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		logger.Log("msg", "Failed to create cache", "err", err)
		os.Exit(1)
	}
	defer baseCache.Close()

	// Create type-safe caches for different types
	userCache := cache.NewGeneric[User](baseCache)
	configCache := cache.NewGeneric[Config](baseCache)
	productCache := cache.NewGeneric[Product](baseCache)
	stringCache := cache.NewGeneric[string](baseCache)

	fmt.Println("=== Generic Cache Demo ===")

	// 1. User Cache Example
	fmt.Println("1. User Cache Example:")

	// User fetcher - simulates database lookup
	userFetcher := cache.GenericFetcher[User](func(ctx context.Context, oldMetadata *daramjwee.Metadata) (User, *daramjwee.Metadata, error) {
		fmt.Println("  📡 Fetching user data from database...")
		return User{
			ID:       1,
			Name:     "John Doe",
			Email:    "john@example.com",
			LastSeen: time.Now(),
		}, &daramjwee.Metadata{ETag: "user-v1"}, nil
	})

	// First get - will fetch from "database"
	user, err := userCache.Get(ctx, "user:1", userFetcher)
	if err != nil {
		fmt.Printf("Failed to get user: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  👤 User: %s (%s)\n", user.Name, user.Email)

	// Second get - will use cache
	fmt.Println("  🔄 Getting from cache again...")
	user2, err := userCache.Get(ctx, "user:1", userFetcher)
	if err != nil {
		fmt.Printf("Failed to get user from cache: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  ✅ Cached user: %s\n", user2.Name)

	// 2. Config Cache Example
	fmt.Println("\n2. Config Cache Example:")

	config := Config{
		AppName:     "MyApp",
		Debug:       true,
		Port:        8080,
		MaxUsers:    1000,
		DatabaseURL: "postgres://localhost/myapp",
	}

	// Set config directly
	err = configCache.Set(ctx, "app-config", config, &daramjwee.Metadata{ETag: "config-v1"})
	if err != nil {
		fmt.Printf("Failed to set config: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("  ⚙️  Configuration saved")

	// Get config using GetOrSet pattern
	retrievedConfig, err := configCache.GetOrSet(ctx, "app-config", func() (Config, *daramjwee.Metadata, error) {
		fmt.Println("  🏭 Creating default configuration...")
		return Config{AppName: "DefaultApp"}, &daramjwee.Metadata{ETag: "default"}, nil
	})
	if err != nil {
		fmt.Printf("Failed to get config: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  📋 App name: %s, Port: %d\n", retrievedConfig.AppName, retrievedConfig.Port)

	// 3. Product Cache Example
	fmt.Println("\n3. Product Cache Example:")

	productFetcher := cache.GenericFetcher[Product](func(ctx context.Context, oldMetadata *daramjwee.Metadata) (Product, *daramjwee.Metadata, error) {
		fmt.Println("  🛒 Fetching product data from API...")
		return Product{
			ID:          "prod-123",
			Name:        "Wireless Earbuds",
			Price:       99.99,
			Category:    "Electronics",
			InStock:     true,
			Description: "High-quality wireless earbuds",
		}, &daramjwee.Metadata{ETag: "product-v1"}, nil
	})

	product, err := productCache.Get(ctx, "product:prod-123", productFetcher)
	if err != nil {
		fmt.Printf("Failed to get product: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  🎧 Product: %s - $%.2f (%s)\n", product.Name, product.Price, product.Category)

	// 4. String Cache Example
	fmt.Println("\n4. String Cache Example:")

	// Using Must methods for simple operations
	stringCache.MustSet(ctx, "welcome-message", "Hello, Generic Cache!", &daramjwee.Metadata{ETag: "msg-v1"})

	welcomeMsg := stringCache.GetWithDefault(ctx, "welcome-message", "Default message", cache.GenericFetcher[string](func(ctx context.Context, oldMetadata *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
		return "Default message", &daramjwee.Metadata{ETag: "default"}, nil
	}))
	fmt.Printf("  💬 Welcome message: %s\n", welcomeMsg)

	// 5. Cache Operations
	fmt.Println("\n5. Cache Operations Example:")

	// Delete a key
	fmt.Println("  🗑️  Deleting user cache...")
	err = userCache.Delete(ctx, "user:1")
	if err != nil {
		fmt.Printf("Failed to delete user: %v\n", err)
		os.Exit(1)
	}

	// Verify deletion by fetching again
	fmt.Println("  🔄 Fetching again after deletion...")
	user3, err := userCache.Get(ctx, "user:1", userFetcher)
	if err != nil {
		fmt.Printf("Failed to get user after delete: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  👤 Newly fetched user: %s\n", user3.Name)

	// 6. Advanced Usage - Schedule Refresh
	fmt.Println("\n6. Advanced Feature - Background Refresh:")

	err = productCache.ScheduleRefresh(ctx, "product:prod-123", productFetcher)
	if err != nil {
		fmt.Printf("Failed to schedule refresh: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("  🔄 Product background refresh scheduled")

	// Give some time for background refresh
	time.Sleep(100 * time.Millisecond)

	// 7. Type Safety Demo
	fmt.Println("\n7. Type Safety Demo:")
	fmt.Println("  ✅ Compile-time type checking")
	fmt.Println("  ✅ Automatic JSON serialization/deserialization")
	fmt.Println("  ✅ Automatic hot/cold store utilization")
	fmt.Println("  ✅ All existing daramjwee features available")

	fmt.Println("\n🎉 Generic Cache Demo Complete!")
	fmt.Printf("📊 Memory Store (Hot): Fast access\n")
	fmt.Printf("💾 File Store (Cold): Persistent storage at %s\n", baseDir)
}
