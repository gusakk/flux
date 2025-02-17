package execute

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute/table"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/internal/jaeger"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/plan"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

type Transport interface {
	Transformation
	// Finished reports when the Transport has completed and there is no more work to do.
	Finished() <-chan struct{}
}

// consecutiveTransport implements Transport by transporting data consecutively to the downstream Transformation.
type consecutiveTransport struct {
	ctx        context.Context
	dispatcher Dispatcher
	logger     *zap.Logger

	t        Transformation
	messages MessageQueue
	stack    []interpreter.StackEntry

	finished chan struct{}
	errMu    sync.Mutex
	errValue error

	schedulerState int32
	inflight       int32
}

func newConsecutiveTransport(ctx context.Context, dispatcher Dispatcher, t Transformation, n plan.Node, logger *zap.Logger) *consecutiveTransport {
	return &consecutiveTransport{
		ctx:        ctx,
		dispatcher: dispatcher,
		logger:     logger,
		t:          t,
		// TODO(nathanielc): Have planner specify message queue initial buffer size.
		messages: newMessageQueue(64),
		stack:    n.CallStack(),
		finished: make(chan struct{}),
	}
}

func (t *consecutiveTransport) sourceInfo() string {
	if len(t.stack) == 0 {
		return ""
	}

	// Learn the filename from the bottom of the stack.
	// We want the top most entry (deepest in the stack)
	// from the primary file. We can retrieve the filename
	// for the primary file by looking at the bottom of the
	// stack and then finding the top-most entry with that
	// filename.
	filename := t.stack[len(t.stack)-1].Location.File
	for i := 0; i < len(t.stack); i++ {
		entry := t.stack[i]
		if entry.Location.File == filename {
			return fmt.Sprintf("@%s: %s", entry.Location, entry.FunctionName)
		}
	}
	entry := t.stack[0]
	return fmt.Sprintf("@%s: %s", entry.Location, entry.FunctionName)
}
func (t *consecutiveTransport) setErr(err error) {
	t.errMu.Lock()
	msg := "runtime error"
	if srcInfo := t.sourceInfo(); srcInfo != "" {
		msg += " " + srcInfo
	}
	err = errors.Wrap(err, codes.Inherit, msg)
	t.errValue = err
	t.errMu.Unlock()
}
func (t *consecutiveTransport) err() error {
	t.errMu.Lock()
	err := t.errValue
	t.errMu.Unlock()
	return err
}

func (t *consecutiveTransport) Finished() <-chan struct{} {
	return t.finished
}

func (t *consecutiveTransport) RetractTable(id DatasetID, key flux.GroupKey) error {
	select {
	case <-t.finished:
		return t.err()
	default:
	}
	t.pushMsg(&retractTableMsg{
		srcMessage: srcMessage(id),
		key:        key,
	})
	return nil
}

func (t *consecutiveTransport) Process(id DatasetID, tbl flux.Table) error {
	select {
	case <-t.finished:
		return t.err()
	default:
	}
	t.pushMsg(&processMsg{
		srcMessage: srcMessage(id),
		table:      newConsecutiveTransportTable(t, tbl),
	})
	return nil
}

func (t *consecutiveTransport) UpdateWatermark(id DatasetID, time Time) error {
	select {
	case <-t.finished:
		return t.err()
	default:
	}
	t.pushMsg(&updateWatermarkMsg{
		srcMessage: srcMessage(id),
		time:       time,
	})
	return nil
}

func (t *consecutiveTransport) UpdateProcessingTime(id DatasetID, time Time) error {
	select {
	case <-t.finished:
		return t.err()
	default:
	}
	t.pushMsg(&updateProcessingTimeMsg{
		srcMessage: srcMessage(id),
		time:       time,
	})
	return nil
}

func (t *consecutiveTransport) Finish(id DatasetID, err error) {
	select {
	case <-t.finished:
		return
	default:
	}
	t.pushMsg(&finishMsg{
		srcMessage: srcMessage(id),
		err:        err,
	})
}

func (t *consecutiveTransport) pushMsg(m Message) {
	t.messages.Push(m)
	atomic.AddInt32(&t.inflight, 1)
	t.schedule()
}

const (
	// consecutiveTransport schedule states
	idle int32 = iota
	running
	finished
)

// schedule indicates that there is work available to schedule.
func (t *consecutiveTransport) schedule() {
	if t.tryTransition(idle, running) {
		t.dispatcher.Schedule(t.processMessages)
	}
}

// tryTransition attempts to transition into the new state and returns true on success.
func (t *consecutiveTransport) tryTransition(old, new int32) bool {
	return atomic.CompareAndSwapInt32(&t.schedulerState, old, new)
}

// transition sets the new state.
func (t *consecutiveTransport) transition(new int32) {
	atomic.StoreInt32(&t.schedulerState, new)
}

func (t *consecutiveTransport) processMessages(ctx context.Context, throughput int) {
PROCESS:
	i := 0
	for m := t.messages.Pop(); m != nil; m = t.messages.Pop() {
		atomic.AddInt32(&t.inflight, -1)
		if f, err := processMessage(ctx, t.t, m); err != nil || f {
			// Set the error if there was any
			t.setErr(err)

			// Transition to the finished state.
			if t.tryTransition(running, finished) {
				// Call Finish if we have not already
				if !f {
					t.t.Finish(m.SrcDatasetID(), t.err())
				}
				// We are finished
				close(t.finished)
				return
			}
		}
		i++
		if i >= throughput {
			// We have done enough work.
			// Transition to the idle state and reschedule for later.
			t.transition(idle)
			t.schedule()
			return
		}
	}

	t.transition(idle)
	// Check if more messages arrived after the above loop finished.
	// This check must happen in the idle state.
	if atomic.LoadInt32(&t.inflight) > 0 {
		if t.tryTransition(idle, running) {
			goto PROCESS
		} // else we have already been scheduled again, we can return
	}
}

func (t *consecutiveTransport) Label() string {
	return t.t.Label()
}

func (t *consecutiveTransport) SetLabel(label string) {
	t.t.SetLabel(label)
}

// processMessage processes the message on t.
// The return value is true if the message was a FinishMsg.
func processMessage(ctx context.Context, t Transformation, m Message) (finished bool, err error) {
	switch m := m.(type) {
	case RetractTableMsg:
		err = t.RetractTable(m.SrcDatasetID(), m.Key())
	case ProcessMsg:
		b := m.Table()
		_, span := StartSpanFromContext(ctx, reflect.TypeOf(t).String(), t.Label())
		err = t.Process(m.SrcDatasetID(), b)
		if span != nil {
			span.Finish()
		}
	case UpdateWatermarkMsg:
		err = t.UpdateWatermark(m.SrcDatasetID(), m.WatermarkTime())
	case UpdateProcessingTimeMsg:
		err = t.UpdateProcessingTime(m.SrcDatasetID(), m.ProcessingTime())
	case FinishMsg:
		t.Finish(m.SrcDatasetID(), m.Error())
		finished = true
	}
	return
}

type Message interface {
	Type() MessageType
	SrcDatasetID() DatasetID
}

type MessageType int

const (
	RetractTableType MessageType = iota
	ProcessType
	UpdateWatermarkType
	UpdateProcessingTimeType
	FinishType
)

type srcMessage DatasetID

func (m srcMessage) SrcDatasetID() DatasetID {
	return DatasetID(m)
}

type RetractTableMsg interface {
	Message
	Key() flux.GroupKey
}

type retractTableMsg struct {
	srcMessage
	key flux.GroupKey
}

func (m *retractTableMsg) Type() MessageType {
	return RetractTableType
}
func (m *retractTableMsg) Key() flux.GroupKey {
	return m.key
}

type ProcessMsg interface {
	Message
	Table() flux.Table
}

type processMsg struct {
	srcMessage
	table flux.Table
}

func (m *processMsg) Type() MessageType {
	return ProcessType
}
func (m *processMsg) Table() flux.Table {
	return m.table
}

type UpdateWatermarkMsg interface {
	Message
	WatermarkTime() Time
}

type updateWatermarkMsg struct {
	srcMessage
	time Time
}

func (m *updateWatermarkMsg) Type() MessageType {
	return UpdateWatermarkType
}
func (m *updateWatermarkMsg) WatermarkTime() Time {
	return m.time
}

type UpdateProcessingTimeMsg interface {
	Message
	ProcessingTime() Time
}

type updateProcessingTimeMsg struct {
	srcMessage
	time Time
}

func (m *updateProcessingTimeMsg) Type() MessageType {
	return UpdateProcessingTimeType
}
func (m *updateProcessingTimeMsg) ProcessingTime() Time {
	return m.time
}

type FinishMsg interface {
	Message
	Error() error
}

type finishMsg struct {
	srcMessage
	err error
}

func (m *finishMsg) Type() MessageType {
	return FinishType
}
func (m *finishMsg) Error() error {
	return m.err
}

// consecutiveTransportTable is a flux.Table that is being processed
// within a consecutiveTransport.
type consecutiveTransportTable struct {
	transport *consecutiveTransport
	tbl       flux.Table
}

func newConsecutiveTransportTable(t *consecutiveTransport, tbl flux.Table) flux.Table {
	return &consecutiveTransportTable{
		transport: t,
		tbl:       tbl,
	}
}

func (t *consecutiveTransportTable) Key() flux.GroupKey {
	return t.tbl.Key()
}

func (t *consecutiveTransportTable) Cols() []flux.ColMeta {
	return t.tbl.Cols()
}

func (t *consecutiveTransportTable) Do(f func(flux.ColReader) error) error {
	return t.tbl.Do(func(cr flux.ColReader) error {
		if err := t.validate(cr); err != nil {
			fields := []zap.Field{
				zap.String("source", t.transport.sourceInfo()),
				zap.Error(err),
			}

			ctx, logger := t.transport.ctx, t.transport.logger
			if span := opentracing.SpanFromContext(ctx); span != nil {
				if traceID, sampled, found := jaeger.InfoFromSpan(span); found {
					fields = append(fields,
						zap.String("tracing/id", traceID),
						zap.Bool("tracing/sampled", sampled),
					)
				}
			}
			logger.Info("Invalid column reader received from predecessor", fields...)
		}
		return f(cr)
	})
}

func (t *consecutiveTransportTable) Done() {
	t.tbl.Done()
}

func (t *consecutiveTransportTable) Empty() bool {
	return t.tbl.Empty()
}

func (t *consecutiveTransportTable) validate(cr flux.ColReader) error {
	if len(cr.Cols()) == 0 {
		return nil
	}

	sz := table.Values(cr, 0).Len()
	for i, n := 1, len(cr.Cols()); i < n; i++ {
		nsz := table.Values(cr, i).Len()
		if sz != nsz {
			// Mismatched column lengths.
			// Look at all column lengths so we can give a more complete
			// error message.
			// We avoid this in the usual case to avoid allocating an array
			// of lengths for every table when it might not be needed.
			lens := make(map[string]int, len(cr.Cols()))
			for i, col := range cr.Cols() {
				label := fmt.Sprintf("%s:%s", col.Label, col.Type)
				lens[label] = table.Values(cr, i).Len()
			}
			return errors.Newf(codes.Internal, "mismatched column lengths: %v", lens)
		}
	}
	return nil
}
