package coordinator_test

import (
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
)

// Plan §14 budgets: one attempt fits in the routing budget, and the routing
// budget stays under the caller's 5 s HTTP budget.
func TestNewValidatesBudgets(t *testing.T) {
	rg := loadRing(t)
	for name, cfg := range map[string]coordinator.Config{
		"budget at the HTTP budget":    {Budget: 5 * time.Second},
		"budget above the HTTP budget": {Budget: 6 * time.Second, AttemptTimeout: time.Second},
		"attempt longer than budget":   {Budget: 2 * time.Second, AttemptTimeout: 2500 * time.Millisecond},
	} {
		cfg.Ring = rg
		if _, err := coordinator.New(cfg); err == nil {
			t.Errorf("%s: New accepted Budget %s, AttemptTimeout %s", name, cfg.Budget, cfg.AttemptTimeout)
		}
	}
	// A short budget with no explicit attempt timeout caps the attempt at the budget.
	if _, err := coordinator.New(coordinator.Config{Ring: rg, Budget: 2 * time.Second}); err != nil {
		t.Fatalf("Budget 2s with the default attempt timeout: %v", err)
	}
	if _, err := coordinator.New(coordinator.Config{Ring: rg}); err != nil {
		t.Fatalf("defaults: %v", err)
	}
}
