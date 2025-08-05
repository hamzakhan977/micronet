package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Job struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Payload  map[string]interface{} `json:"payload,omitempty"`
	Status   string                 `json:"status"`
	Attempts int                    `json:"attempts"`
	MaxTries int                    `json:"max_tries"`
	Created  time.Time              `json:"created_at"`
	Updated  time.Time              `json:"updated_at"`
}

var (
	queueKey = "micronet:queue"
	jobKey   = "micronet:job"
	dlqKey   = "micronet:dead_letter"
)

func main() {
	// config from env
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	workerConcurrency := 1
	// optional env WORKER_CONCURRENCY
	if v := os.Getenv("WORKER_CONCURRENCY"); v != "" {
		if n, err := strconvAtoiSafe(v); err == nil && n > 0 {
			workerConcurrency = n
		}
	}

	// Redis client
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	// quick ping
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}

	// Start worker(s)
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := 0; i < workerConcurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			workerLoop(workerCtx, rdb, id)
		}(i + 1)
	}

	// Gin setup
	gin.SetMode(gin.DebugMode)
	router := gin.Default()

	router.POST("/jobs", func(c *gin.Context) {
		var body map[string]interface{}
		if err := c.BindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Create job
		j := Job{
			ID:       uuid.NewString(),
			Type:     "",
			Payload:  body,
			Status:   "queued",
			Attempts: 0,
			MaxTries: 3,
			Created:  time.Now().UTC(),
			Updated:  time.Now().UTC(),
		}
		if t, ok := body["type"].(string); ok {
			j.Type = t
			delete(j.Payload, "type")
		}

		if err := saveJob(ctx, rdb, &j); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if err := rdb.RPush(ctx, queueKey, j.ID).Err(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "job queued", "id": j.ID})
	})

	router.GET("/jobs/:id/status", func(c *gin.Context) {
		id := c.Param("id")
		j, err := getJob(ctx, rdb, id)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"id": j.ID, "status": j.Status, "attempts": j.Attempts, "max_tries": j.MaxTries})
	})

	// Optional: view DLQ entries (simple list)
	router.GET("/dlq", func(c *gin.Context) {
		list, err := rdb.LRange(ctx, dlqKey, 0, -1).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		var out []Job
		for _, s := range list {
			var j Job
			if err := json.Unmarshal([]byte(s), &j); err == nil {
				out = append(out, j)
			}
		}
		c.JSON(http.StatusOK, gin.H{"dead_letter": out})
	})

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	// start server
	go func() {
		log.Printf("Micronet API running on :%s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server listen: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	log.Println("Shutdown signal received")

	// stop workers
	cancelWorkers()
	// give workers up to 10s to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Workers shut down cleanly")
	case <-time.After(10 * time.Second):
		log.Println("Workers did not finish in time")
	}

	// shutdown HTTP server
	ctxShut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctxShut); err != nil {
		log.Fatalf("Server Shutdown Failed:%+v", err)
	}
	log.Println("Server exited properly")
}

// workerLoop pops job IDs, loads job, processes with retries and DLQ
func workerLoop(ctx context.Context, rdb *redis.Client, workerID int) {
	log.Printf("worker[%d] started\n", workerID)
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker[%d] stopping (ctx done)\n", workerID)
			return
		default:
			// BLPop with timeout so we can check ctx regularly
			res, err := rdb.BLPop(ctx, 5*time.Second, queueKey).Result()
			if err != nil {
				// ignore timeout or context cancel
				if err == redis.Nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					continue
				}
				log.Printf("worker[%d] blpop error: %v\n", workerID, err)
				continue
			}
			if len(res) < 2 {
				continue
			}
			jobID := res[1]
			j, err := getJob(ctx, rdb, jobID)
			if err != nil {
				log.Printf("worker[%d] getJob error: %v\n", workerID, err)
				continue
			}

			// mark processing
			j.Attempts++
			j.Status = "processing"
			j.Updated = time.Now().UTC()
			if err := saveJob(ctx, rdb, j); err != nil {
				log.Printf("worker[%d] saveJob error: %v\n", workerID, err)
			}

			log.Printf("worker[%d] processing id=%s type=%s attempt=%d\n", workerID, j.ID, j.Type, j.Attempts)

			// process
			if err := processJob(j); err != nil {
				log.Printf("worker[%d] job id=%s failed: %v\n", workerID, j.ID, err)
				if j.Attempts < j.MaxTries {
					// requeue for retry
					if err := rdb.RPush(ctx, queueKey, j.ID).Err(); err != nil {
						log.Printf("worker[%d] requeue failed: %v\n", workerID, err)
					} else {
						j.Status = "queued"
						j.Updated = time.Now().UTC()
						if err := saveJob(ctx, rdb, j); err != nil {
							log.Printf("worker[%d] saveJob after requeue failed: %v\n", workerID, err)
						}
						log.Printf("worker[%d] requeued id=%s\n", workerID, j.ID)
					}
				} else {
					// move to DLQ
					j.Status = "failed"
					j.Updated = time.Now().UTC()
					if err := saveJob(ctx, rdb, j); err != nil {
						log.Printf("worker[%d] saveJob failed marking failed: %v\n", workerID, err)
					}
					raw, _ := json.Marshal(j)
					if err := rdb.RPush(ctx, dlqKey, raw).Err(); err != nil {
						log.Printf("worker[%d] push DLQ failed: %v\n", workerID, err)
					} else {
						log.Printf("worker[%d] moved id=%s to DLQ\n", workerID, j.ID)
					}
				}
				continue
			}

			// success
			j.Status = "done"
			j.Updated = time.Now().UTC()
			if err := saveJob(ctx, rdb, j); err != nil {
				log.Printf("worker[%d] saveJob done error: %v\n", workerID, err)
			}
			log.Printf("worker[%d] done id=%s\n", workerID, j.ID)
		}
	}
}

func saveJob(ctx context.Context, rdb *redis.Client, j *Job) error {
	raw, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return rdb.Set(ctx, jobKey+":"+j.ID, raw, 0).Err()
}

func getJob(ctx context.Context, rdb *redis.Client, id string) (*Job, error) {
	raw, err := rdb.Get(ctx, jobKey+":"+id).Result()
	if err != nil {
		return nil, err
	}
	var j Job
	if err := json.Unmarshal([]byte(raw), &j); err != nil {
		return nil, err
	}
	return &j, nil
}

// processJob performs the actual work. For demo, we simulate failure
// when job.Type == "email" so you can see retries -> DLQ. Replace with real logic.
func processJob(j *Job) error {
	// Simulated failure policy:
	if j.Type == "email" {
		return errors.New("simulated failure for email jobs")
	}
	// simulate work
	time.Sleep(1 * time.Second)
	return nil
}

// helper: safe atoi
func strconvAtoiSafe(s string) (int, error) {
	return strconv.Atoi(s)
}
