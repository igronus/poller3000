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

type Target struct {
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	Namespace string `yaml:"namespace"`
}

type Config struct {
	Temporal struct {
		Targets []Target `yaml:"targets"`
	} `yaml:"temporal"`
	Monitoring struct {
		PollIntervalSeconds int `yaml:"poll_interval_seconds"`
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
	}

	return &cfg, nil
}

type TemporalClient struct {
	Client    client.Client
	Service   workflowservice.WorkflowServiceClient
	Host      string
	Port      int
	Namespace string
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
		Client:    c,
		Service:   c.WorkflowService(),
		Host:      target.Host,
		Port:      target.Port,
		Namespace: target.Namespace,
	}, nil
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
		}
	}
}

func checkCompletedWithPollers(ctx context.Context, tc *TemporalClient) {
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

			pollerCount, err := getTaskQueuePollers(ctx, tc.Service, tc.Namespace, wf.TaskQueue)
			if err != nil {
				log.Printf("[%s/%s] Error getting pollers for task queue %s: %v", tc.Host, tc.Namespace, wf.TaskQueue, err)
				continue
			}

			if pollerCount > 0 {
				log.Printf("[ALERT] [%s/%s] %s workflow '%s' (type: %s) on task queue '%s' still has %d poller(s)",
					tc.Host, tc.Namespace, status, wf.WorkflowID, wf.WorkflowType, wf.TaskQueue, pollerCount)
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

	ticker := time.NewTicker(time.Duration(cfg.Monitoring.PollIntervalSeconds) * time.Second)
	defer ticker.Stop()

	log.Printf("Starting monitoring (poll interval: %d seconds, %d targets)",
		cfg.Monitoring.PollIntervalSeconds, len(clients))

	for {
		select {
		case <-ticker.C:
			log.Println("Checking for issues...")
			for _, tc := range clients {
				checkRunningWithNoPollers(ctx, tc)
				checkCompletedWithPollers(ctx, tc)
			}
		case <-sigChan:
			log.Println("Shutting down...")
			return
		}
	}
}
