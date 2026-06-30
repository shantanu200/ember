package quelon

import (
	"fmt"
	"time"
)

type opKind uint8

const (
	opSave opKind = iota
	opDelete
	opDeadLetter
)

// storeOp is a single pending-store mutation queued to the group-commit writer.
type storeOp struct {
	kind opKind
	task RawTask       // opSave
	id   string        // opDelete
	dl   RawDeadLetter // opDeadLetter
}

// opSet accumulates the mutations of one flush window. Saves are deduplicated by
// id — the latest carried Attempt wins — so a task saved several times across
// retries commits once; deletes are a set and dead letters accumulate. The whole
// window is then applied with a single durable commit.
type opSet struct {
	saves   map[string]RawTask
	deletes map[string]struct{}
	dls     []RawDeadLetter
}

func newOpSet() *opSet {
	return &opSet{
		saves:   make(map[string]RawTask),
		deletes: make(map[string]struct{}),
	}
}

func (s *opSet) add(op storeOp) {
	switch op.kind {
	case opSave:
		s.saves[op.task.ID] = op.task
	case opDelete:
		s.deletes[op.id] = struct{}{}
	case opDeadLetter:
		s.dls = append(s.dls, op.dl)
	}
}

func (s *opSet) len() int { return len(s.saves) + len(s.deletes) + len(s.dls) }

// queueOp hands a mutation to the group-commit writer. It blocks only when the
// writer has fallen behind (the op buffer is full) — bounded backpressure that
// signals the store can't keep up, rather than silently dropping durable state.
// The writer outlives every queueOp caller (p.ops is closed only after all
// workers finish, in CloseAndWait), so this send always eventually proceeds.
func (p *Pool) queueOp(op storeOp) {
	p.ops <- op
}

// commitLoop is the group-commit writer. It owns every pending-store mutation,
// accumulating ops into a window and flushing — one durable commit per window —
// when the window reaches flushSize or flushEvery elapses, whichever comes
// first. Because it is the single writer applying ops in order, save-before-
// delete and dead-letter-before-delete ordering hold without per-task locking.
func (p *Pool) commitLoop() {
	defer close(p.writerDone)

	set := newOpSet()
	timer := time.NewTimer(p.flushEvery)
	defer timer.Stop()

	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(p.flushEvery)
	}

	for {
		select {
		case op, ok := <-p.ops:
			if !ok {
				p.commit(set) // drain the final window on shutdown
				return
			}
			set.add(op)
			if set.len() >= p.flushSize {
				p.commit(set)
				set = newOpSet()
				resetTimer()
			}
		case <-timer.C:
			p.commit(set)
			set = newOpSet()
			timer.Reset(p.flushEvery)
		}
	}
}

// commit applies one window. When the store implements CommitStore the whole
// window lands in a single atomic, single-fsync call; otherwise it falls back to
// per-item Store calls, applied save -> dead letter -> delete so a failed task
// is durable as a dead letter before its pending record is removed.
func (p *Pool) commit(set *opSet) {
	if set.len() == 0 {
		return
	}

	saves := make([]RawTask, 0, len(set.saves))
	for _, t := range set.saves {
		saves = append(saves, t)
	}
	deletes := make([]string, 0, len(set.deletes))
	for id := range set.deletes {
		deletes = append(deletes, id)
	}

	if cs, ok := p.store.(CommitStore); ok {
		if err := cs.Commit(saves, deletes, set.dls); err != nil {
			p.storeErr(fmt.Errorf("group commit (%d saves, %d deletes, %d dead letters): %w",
				len(saves), len(deletes), len(set.dls), err))
		}
		return
	}

	for _, t := range saves {
		if err := p.store.SaveTask(t); err != nil {
			p.storeErr(fmt.Errorf("persisting task %s: %w", t.ID, err))
		}
	}
	if len(set.dls) > 0 {
		p.saveDeadLetters(set.dls)
	}
	if len(deletes) > 0 {
		p.deleteTasks(deletes)
	}
}

func (p *Pool) storeErr(err error) {
	if p.hooks.OnStoreError != nil {
		p.hooks.OnStoreError(err)
	}
}
