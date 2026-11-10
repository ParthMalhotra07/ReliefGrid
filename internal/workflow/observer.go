package workflow

import "github.com/ParthMalhotra07/ReliefGrid/internal/store"

// Observer receives process-local observations. Implementations must not block,
// perform remote I/O, mutate inputs, or influence any workflow decision.
type Observer interface {
	Attempt(outcome string, err error)
	Transition(before store.Request, command store.TransitionRequest, result store.Result)
}
