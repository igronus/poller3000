package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"gopkg.in/yaml.v3"
)

type Handlers struct {
	LostWorker      string `yaml:"lost_worker"`
	RedundantWorker string `yaml:"redundant_worker"`
}

type Target struct {
	Host                string   `yaml:"host"`
	Port                int      `yaml:"port"`
	Namespace           string   `yaml:"namespace"`
	PollIntervalSeconds int      `yaml:"poll_interval_seconds"`
	Handlers            Handlers `yaml:"handlers"`
}

type Config struct {
	Temporal struct {
		Targets []Target `yaml:"targets"`
	} `yaml:"temporal"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	for i := range cfg.Temporal.Targets {
		if cfg.Temporal.Targets[i].Port == 0 {
			cfg.Temporal.Targets[i].Port = 7233
		}
		if cfg.Temporal.Targets[i].Namespace == "" {
			cfg.Temporal.Targets[i].Namespace = "default"
		}
		if cfg.Temporal.Targets[i].Host == "" {
			cfg.Temporal.Targets[i].Host = "localhost"
		}
		if cfg.Temporal.Targets[i].PollIntervalSeconds == 0 {
			cfg.Temporal.Targets[i].PollIntervalSeconds = 30
		}
	}

	return &cfg, nil
}

type TemporalClient struct {
	Client              client.Client
	Service             workflowservice.WorkflowServiceClient
	Host                string
	Port                int
	Namespace           string
	Handlers            Handlers
	PollIntervalSeconds int
}

func connectToTarget(target Target) (*TemporalClient, error) {
	addr := fmt.Sprintf("%s:%d", target.Host, target.Port)

	c, err := client.Dial(client.Options{
		HostPort:  addr,
		Namespace: target.Namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	return &TemporalClient{
		Client:              c,
		Service:             c.WorkflowService(),
		Host:                target.Host,
		Port:                target.Port,
		Namespace:           target.Namespace,
		Handlers:            target.Handlers,
		PollIntervalSeconds: target.PollIntervalSeconds,
	}, nil
}

type LostWorkerPayload struct {
	Host         string `json:"host"`
	Port         int    `json:"port"`
	Namespace    string `json:"namespace"`
	WorkflowID   string `json:"workflow_id"`
	RunID        string `json:"run_id"`
	WorkflowType string `json:"workflow_type"`
	TaskQueue    string `json:"task_queue"`
}

type RedundantWorkerPayload struct {
	Host         string `json:"host"`
	Port         int    `json:"port"`
	Namespace    string `json:"namespace"`
	WorkflowID   string `json:"workflow_id"`
	RunID        string `json:"run_id"`
	WorkflowType string `json:"workflow_type"`
	TaskQueue    string `json:"task_queue"`
	Status       string `json:"status"`
	PollerCount  int    `json:"poller_count"`
}

func callHandler(handler string, payload interface{}) {
	if handler == "" {
		return
	}

	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshaling payload: %v", err)
		return
	}

	if strings.HasPrefix(handler, "http://") || strings.HasPrefix(handler, "https://") {
		resp, err := http.Post(handler, "application/json", bytes.NewBuffer(body))
		if err != nil {
			log.Printf("Error calling handler %s: %v", handler, err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			log.Printf("Handler %s returned error status: %d", handler, resp.StatusCode)
		} else {
			log.Printf("Handler %s called successfully", handler)
		}
	} else {
		cmd := exec.Command(handler, string(body))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Printf("Error executing handler %s: %v", handler, err)
		} else {
			log.Printf("Handler %s executed successfully", handler)
		}
	}
}

func getTaskQueuePollers(ctx context.Context, service workflowservice.WorkflowServiceClient, namespace, taskQueue string) (int, error) {
	resp, err := service.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     namespace,
		TaskQueue:     &taskqueue.TaskQueue{Name: taskQueue},
		ReportPollers: true,
	})
	if err != nil {
		return 0, err
	}

	pollers := resp.GetPollers()
	return len(pollers), nil
}

type WorkflowInfo struct {
	WorkflowID   string
	RunID        string
	WorkflowType string
	TaskQueue    string
	Status       string
}

func getWorkflowsByStatus(ctx context.Context, c client.Client, namespace, status string) ([]*WorkflowInfo, error) {
	resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: namespace,
		Query:     fmt.Sprintf("ExecutionStatus = '%s'", status),
	})
	if err != nil {
		return nil, err
	}

	var workflows []*WorkflowInfo
	for _, exec := range resp.Executions {
		workflows = append(workflows, &WorkflowInfo{
			WorkflowID:   exec.GetExecution().GetWorkflowId(),
			RunID:        exec.GetExecution().GetRunId(),
			WorkflowType: exec.GetType().GetName(),
			TaskQueue:    exec.GetTaskQueue(),
			Status:       exec.GetStatus().String(),
		})
	}

	return workflows, nil
}

func checkRunningWithNoPollers(ctx context.Context, tc *TemporalClient) {
	runningWorkflows, err := getWorkflowsByStatus(ctx, tc.Client, tc.Namespace, "Running")
	if err != nil {
		log.Printf("[%s/%s] Error getting running workflows: %v", tc.Host, tc.Namespace, err)
		return
	}

	log.Printf("[%s/%s] Found %d running workflows", tc.Host, tc.Namespace, len(runningWorkflows))

	for _, wf := range runningWorkflows {
		if wf.TaskQueue == "" {
			continue
		}

		pollerCount, err := getTaskQueuePollers(ctx, tc.Service, tc.Namespace, wf.TaskQueue)
		if err != nil {
			log.Printf("[%s/%s] Error getting pollers for task queue %s: %v", tc.Host, tc.Namespace, wf.TaskQueue, err)
			continue
		}

		if pollerCount == 0 {
			log.Printf("[ALERT] [%s/%s] Running workflow '%s' (type: %s) on task queue '%s' has NO pollers",
				tc.Host, tc.Namespace, wf.WorkflowID, wf.WorkflowType, wf.TaskQueue)

			payload := LostWorkerPayload{
				Host:         tc.Host,
				Port:         tc.Port,
				Namespace:    tc.Namespace,
				WorkflowID:   wf.WorkflowID,
				RunID:        wf.RunID,
				WorkflowType: wf.WorkflowType,
				TaskQueue:    wf.TaskQueue,
			}
			go callHandler(tc.Handlers.LostWorker, payload)
		}
	}
}

func checkCompletedWithPollers(ctx context.Context, tc *TemporalClient) {
	runningWorkflows, err := getWorkflowsByStatus(ctx, tc.Client, tc.Namespace, "Running")
	if err != nil {
		log.Printf("[%s/%s] Error getting running workflows: %v", tc.Host, tc.Namespace, err)
		return
	}

	runningTaskQueues := make(map[string]bool)
	for _, wf := range runningWorkflows {
		runningTaskQueues[wf.TaskQueue] = true
	}

	statuses := []string{"Completed", "Terminated", "Canceled", "Failed"}

	for _, status := range statuses {
		workflows, err := getWorkflowsByStatus(ctx, tc.Client, tc.Namespace, status)
		if err != nil {
			log.Printf("[%s/%s] Error getting %s workflows: %v", tc.Host, tc.Namespace, status, err)
			continue
		}

		if len(workflows) == 0 {
			continue
		}

		log.Printf("[%s/%s] Found %d %s workflows", tc.Host, tc.Namespace, len(workflows), status)

		for _, wf := range workflows {
			if wf.TaskQueue == "" {
				continue
			}

			if runningTaskQueues[wf.TaskQueue] {
				continue
			}

			pollerCount, err := getTaskQueuePollers(ctx, tc.Service, tc.Namespace, wf.TaskQueue)
			if err != nil {
				log.Printf("[%s/%s] Error getting pollers for task queue %s: %v", tc.Host, tc.Namespace, wf.TaskQueue, err)
				continue
			}

			if pollerCount > 0 {
				log.Printf("[ALERT] [%s/%s] %s workflow '%s' (type: %s) on task queue '%s' still has %d poller(s)",
					tc.Host, tc.Namespace, status, wf.WorkflowID, wf.WorkflowType, wf.TaskQueue, pollerCount)

				payload := RedundantWorkerPayload{
					Host:         tc.Host,
					Port:         tc.Port,
					Namespace:    tc.Namespace,
					WorkflowID:   wf.WorkflowID,
					RunID:        wf.RunID,
					WorkflowType: wf.WorkflowType,
					TaskQueue:    wf.TaskQueue,
					Status:       status,
					PollerCount:  pollerCount,
				}
				go callHandler(tc.Handlers.RedundantWorker, payload)
			}
		}
	}
}

func main() {
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if len(cfg.Temporal.Targets) == 0 {
		log.Fatal("No targets configured in temporal.targets")
	}

	var clients []*TemporalClient
	for _, target := range cfg.Temporal.Targets {
		tc, err := connectToTarget(target)
		if err != nil {
			log.Printf("Warning: Failed to connect to %s:%d: %v", target.Host, target.Port, err)
			continue
		}
		clients = append(clients, tc)
		log.Printf("Connected to %s:%d, namespace: %s", target.Host, target.Port, target.Namespace)
	}

	if len(clients) == 0 {
		log.Fatal("No clients connected")
	}

	for _, c := range clients {
		defer c.Client.Close()
	}

	ctx := context.Background()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	type clientTicker struct {
		client *TemporalClient
		ticker *time.Ticker
	}

	var tickers []clientTicker
	for _, tc := range clients {
		tickers = append(tickers, clientTicker{
			client: tc,
			ticker: time.NewTicker(time.Duration(tc.PollIntervalSeconds) * time.Second),
		})

		if tc.Handlers.LostWorker != "" {
			log.Printf("[%s/%s] Lost worker handler: %s", tc.Host, tc.Namespace, tc.Handlers.LostWorker)
		}
		if tc.Handlers.RedundantWorker != "" {
			log.Printf("[%s/%s] Redundant worker handler: %s", tc.Host, tc.Namespace, tc.Handlers.RedundantWorker)
		}
	}

	for _, t := range tickers {
		defer t.ticker.Stop()
	}

	log.Printf("Starting monitoring (%d targets)", len(clients))

	for {
		select {
		case <-sigChan:
			log.Println("Shutting down...")
			return
		default:
			for _, ct := range tickers {
				select {
				case <-ct.ticker.C:
					checkRunningWithNoPollers(ctx, ct.client)
					checkCompletedWithPollers(ctx, ct.client)
				default:
				}
			}
			time.Sleep(time.Second)
		}
	}
}
