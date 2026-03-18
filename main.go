package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Temporal struct {
		Host      string `yaml:"host"`
		Port      int    `yaml:"port"`
		Namespace string `yaml:"namespace"`
	} `yaml:"temporal"`
	Monitoring struct {
		PollIntervalSeconds int    `yaml:"poll_interval_seconds"`
		LogLevel            string `yaml:"log_level"`
	} `yaml:"monitoring"`
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

	if cfg.Monitoring.PollIntervalSeconds == 0 {
		cfg.Monitoring.PollIntervalSeconds = 30
	}
	if cfg.Temporal.Host == "" {
		cfg.Temporal.Host = "localhost"
	}
	if cfg.Temporal.Port == 0 {
		cfg.Temporal.Port = 7233
	}
	if cfg.Temporal.Namespace == "" {
		cfg.Temporal.Namespace = "default"
	}

	return &cfg, nil
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

func checkRunningWithNoPollers(ctx context.Context, c client.Client, service workflowservice.WorkflowServiceClient, namespace string) {
	runningWorkflows, err := getWorkflowsByStatus(ctx, c, namespace, "Running")
	if err != nil {
		log.Printf("Error getting running workflows: %v", err)
		return
	}

	log.Printf("Found %d running workflows", len(runningWorkflows))

	for _, wf := range runningWorkflows {
		if wf.TaskQueue == "" {
			continue
		}

		pollerCount, err := getTaskQueuePollers(ctx, service, namespace, wf.TaskQueue)
		if err != nil {
			log.Printf("Error getting pollers for task queue %s: %v", wf.TaskQueue, err)
			continue
		}

		if pollerCount == 0 {
			log.Printf("[ALERT] Running workflow '%s' (type: %s) on task queue '%s' has NO pollers",
				wf.WorkflowID, wf.WorkflowType, wf.TaskQueue)
		}
	}
}

func checkCompletedWithPollers(ctx context.Context, c client.Client, service workflowservice.WorkflowServiceClient, namespace string) {
	statuses := []string{"Completed", "Terminated", "Canceled", "Failed"}

	for _, status := range statuses {
		workflows, err := getWorkflowsByStatus(ctx, c, namespace, status)
		if err != nil {
			log.Printf("Error getting %s workflows: %v", status, err)
			continue
		}

		if len(workflows) == 0 {
			continue
		}

		log.Printf("Found %d %s workflows", len(workflows), status)

		for _, wf := range workflows {
			if wf.TaskQueue == "" {
				continue
			}

			pollerCount, err := getTaskQueuePollers(ctx, service, namespace, wf.TaskQueue)
			if err != nil {
				log.Printf("Error getting pollers for task queue %s: %v", wf.TaskQueue, err)
				continue
			}

			if pollerCount > 0 {
				log.Printf("[ALERT] %s workflow '%s' (type: %s) on task queue '%s' still has %d poller(s)",
					status, wf.WorkflowID, wf.WorkflowType, wf.TaskQueue, pollerCount)
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

	temporalAddr := fmt.Sprintf("%s:%d", cfg.Temporal.Host, cfg.Temporal.Port)

	c, err := client.Dial(client.Options{
		HostPort:  temporalAddr,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		log.Fatalf("Failed to connect to Temporal: %v", err)
	}
	defer c.Close()

	service := c.WorkflowService()

	log.Printf("Connected to Temporal at %s, namespace: %s", temporalAddr, cfg.Temporal.Namespace)

	ctx := context.Background()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(time.Duration(cfg.Monitoring.PollIntervalSeconds) * time.Second)
	defer ticker.Stop()

	log.Printf("Starting monitoring (poll interval: %d seconds)", cfg.Monitoring.PollIntervalSeconds)

	for {
		select {
		case <-ticker.C:
			log.Println("Checking for issues...")
			checkRunningWithNoPollers(ctx, c, service, cfg.Temporal.Namespace)
			checkCompletedWithPollers(ctx, c, service, cfg.Temporal.Namespace)
		case <-sigChan:
			log.Println("Shutting down...")
			return
		}
	}
}
