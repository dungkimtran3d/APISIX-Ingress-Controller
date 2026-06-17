package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"
)

const (
	FinalizerName = "apisix.ingress.apache.org/resources-cleanup"
)

// Resource represents a mock Kubernetes resource (ApisixRoute, ApisixUpstream, etc.)
type Resource struct {
	Name              string
	Namespace         string
	Kind              string
	Finalizers        []string
	DeletionTimestamp *time.Time
}

// ApisixClient simulates the APISIX Admin API client
type ApisixClient struct {
	UnreachableAttempts int
	currentAttempt      int
}

// DeleteResource simulates deleting a resource in APISIX Admin API
func (c *ApisixClient) DeleteResource(ctx context.Context, kind, namespace, name string) (int, error) {
	if c.currentAttempt < c.UnreachableAttempts {
		c.currentAttempt++
		return 0, errors.New("APISIX Admin API is temporarily unreachable")
	}
	// Simulate successful deletion or already deleted
	return http.StatusOK, nil
}

// Controller simulates the Kubernetes controller reconciling resources
type Controller struct {
	apisixClient *ApisixClient
}

func (c *Controller) Reconcile(ctx context.Context, r *Resource) error {
	if r.DeletionTimestamp != nil {
		// Resource is being deleted
		if hasFinalizer(r, FinalizerName) {
			fmt.Printf("[%s/%s] Deletion detected. Cleaning up APISIX resources...\n", r.Namespace, r.Name)
			
			// Delete from APISIX with retry and exponential backoff
			err := c.deleteWithRetry(ctx, r)
			if err != nil {
				return fmt.Errorf("failed to clean up APISIX resources: %w", err)
			}

			// Remove finalizer
			removeFinalizer(r, FinalizerName)
			fmt.Printf("[%s/%s] Finalizer removed successfully.\n", r.Namespace, r.Name)
		}
		return nil
	}

	// Resource is active, ensure finalizer is present
	if !hasFinalizer(r, FinalizerName) {
		r.Finalizers = append(r.Finalizers, FinalizerName)
		fmt.Printf("[%s/%s] Added finalizer %s\n", r.Namespace, r.Name, FinalizerName)
	}

	return nil
}

func (c *Controller) deleteWithRetry(ctx context.Context, r *Resource) error {
	backoff := 100 * time.Millisecond
	maxBackoff := 2 * time.Second
	maxAttempts := 5

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		statusCode, err := c.apisixClient.DeleteResource(ctx, r.Kind, r.Namespace, r.Name)
		if err == nil {
			if statusCode == http.StatusOK || statusCode == http.StatusNotFound {
				fmt.Printf("[%s/%s] Successfully deleted from APISIX (Status: %d)\n", r.Namespace, r.Name, statusCode)
				return nil
			}
			return fmt.Errorf("unexpected status code from APISIX: %d", statusCode)
		}

		fmt.Printf("[%s/%s] Attempt %d failed: %v. Retrying in %v...\n", r.Namespace, r.Name, attempt, err, backoff)
		
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff = time.Duration(math.Min(float64(backoff*2), float64(maxBackoff)))
	}

	return errors.New("max retry attempts reached, APISIX Admin API still unreachable")
}

func hasFinalizer(r *Resource, finalizer string) bool {
	for _, f := range r.Finalizers {
		if f == finalizer {
			return true
		}
	}
	return false
}

func removeFinalizer(r *Resource, finalizer string) {
	var newFinalizers []string
	for _, f := range r.Finalizers {
		if f != finalizer {
			newFinalizers = append(newFinalizers, f)
		}
	}
	r.Finalizers = newFinalizers
}

func main() {
	fmt.Println("Starting APISIX Ingress Controller Namespace Deletion Cleanup Simulation...")

	client := &ApisixClient{UnreachableAttempts: 2}
	controller := &Controller{apisixClient: client}

	// 1. Create a resource
	route := &Resource{
		Name:      "echo-route",
		Namespace: "test-cleanup",
		Kind:      "ApisixRoute",
	}

	ctx := context.Background()

	// Reconcile active resource (adds finalizer)
	_ = controller.Reconcile(ctx, route)

	// 2. Simulate namespace deletion (sets DeletionTimestamp)
	now := time.Now()
	route.DeletionTimestamp = &now

	// Reconcile deleting resource (triggers cleanup and finalizer removal)
	err := controller.Reconcile(ctx, route)
	if err != nil {
		fmt.Printf("Reconciliation failed: %v\n", err)
	} else {
		fmt.Println("Reconciliation completed successfully.")
	}
}
