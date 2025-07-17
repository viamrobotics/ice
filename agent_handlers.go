// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package ice

import (
	"context"
	"sync"
)

// OnConnectionStateChange sets a handler that is fired when the connection state changes
func (a *Agent) OnConnectionStateChange(f func(ConnectionState)) error {
	a.onConnectionStateChangeHdlr.Store(f)
	return nil
}

// OnSelectedCandidatePairChange sets a handler that is fired when the final candidate
// pair is selected
func (a *Agent) OnSelectedCandidatePairChange(f func(Candidate, Candidate)) error {
	a.onSelectedCandidatePairChangeHdlr.Store(f)
	return nil
}

// OnCandidate sets a handler that is fired when new candidates gathered. When
// the gathering process complete the last candidate is nil.
func (a *Agent) OnCandidate(f func(Candidate)) error {
	a.onCandidateHdlr.Store(f)
	return nil
}

func (a *Agent) onSelectedCandidatePairChange(p *CandidatePair) {
	if h, ok := a.onSelectedCandidatePairChangeHdlr.Load().(func(Candidate, Candidate)); ok {
		h(p.Local, p.Remote)
	}
}

func (a *Agent) onCandidate(c Candidate) {
	if onCandidateHdlr, ok := a.onCandidateHdlr.Load().(func(Candidate)); ok {
		onCandidateHdlr(c)
	}
}

func (a *Agent) onConnectionStateChange(s ConnectionState) {
	if hdlr, ok := a.onConnectionStateChangeHdlr.Load().(func(ConnectionState)); ok {
		hdlr(s)
	}
}

type handlerNotifier struct {
	sync.Mutex
	running bool

	notifiers       sync.WaitGroup
	notifiersCtx    context.Context
	notifiersCancel context.CancelFunc

	connectionStates    []ConnectionState
	connectionStateFunc func(ConnectionState)

	candidates    []Candidate
	candidateFunc func(Candidate)

	selectedCandidatePairs []*CandidatePair
	candidatePairFunc      func(*CandidatePair)
}

// Creates a new handler notifier. Only one of the passed-in callbacks should be
// specified.
func newHandlerNotifier(
	connectionStateFunc func(ConnectionState),
	candidateFunc func(Candidate),
	candidatePairFunc func(*CandidatePair),
) *handlerNotifier {
	notifiersCtx, notifiersCancel := context.WithCancel(context.Background())
	return &handlerNotifier{
		notifiersCtx:        notifiersCtx,
		notifiersCancel:     notifiersCancel,
		connectionStateFunc: connectionStateFunc,
		candidateFunc:       candidateFunc,
		candidatePairFunc:   candidatePairFunc,
	}
}

func (h *handlerNotifier) Close(graceful bool) {
	h.Lock()
	if h.notifiersCtx.Err() != nil {
		h.Unlock()
		return
	}
	h.notifiersCancel()
	h.Unlock()

	if graceful {
		h.notifiers.Wait()
	}
}

func (h *handlerNotifier) EnqueueConnectionState(s ConnectionState) {
	h.Lock()
	if h.notifiersCtx.Err() != nil {
		h.Unlock()
		return
	}

	notify := func() {
		defer h.notifiers.Done()
		for {
			h.Lock()
			if len(h.connectionStates) == 0 {
				h.running = false
				h.Unlock()
				return
			}
			notification := h.connectionStates[0]
			h.connectionStates = h.connectionStates[1:]
			h.Unlock()
			h.connectionStateFunc(notification)
		}
	}

	h.connectionStates = append(h.connectionStates, s)
	if !h.running {
		h.running = true
		h.notifiers.Add(1)
		h.Unlock()

		go notify()
	} else {
		h.Unlock()
	}
}

func (h *handlerNotifier) EnqueueCandidate(c Candidate) {
	h.Lock()
	if h.notifiersCtx.Err() != nil {
		h.Unlock()
		return
	}

	notify := func() {
		defer h.notifiers.Done()
		for {
			h.Lock()
			if len(h.candidates) == 0 {
				h.running = false
				h.Unlock()
				return
			}
			notification := h.candidates[0]
			h.candidates = h.candidates[1:]
			h.Unlock()
			h.candidateFunc(notification)
		}
	}

	h.candidates = append(h.candidates, c)
	if !h.running {
		h.running = true
		h.notifiers.Add(1)
		h.Unlock()

		go notify()
	} else {
		h.Unlock()
	}
}

func (h *handlerNotifier) EnqueueSelectedCandidatePair(p *CandidatePair) {
	h.Lock()
	if h.notifiersCtx.Err() != nil {
		h.Unlock()
		return
	}

	notify := func() {
		defer h.notifiers.Done()
		for {
			h.Lock()
			if len(h.selectedCandidatePairs) == 0 {
				h.running = false
				h.Unlock()
				return
			}
			notification := h.selectedCandidatePairs[0]
			h.selectedCandidatePairs = h.selectedCandidatePairs[1:]
			h.Unlock()

			h.candidatePairFunc(notification)
		}
	}

	h.selectedCandidatePairs = append(h.selectedCandidatePairs, p)
	if !h.running {
		h.running = true
		h.notifiers.Add(1)
		h.Unlock()

		go notify()
	} else {
		h.Unlock()
	}
}
