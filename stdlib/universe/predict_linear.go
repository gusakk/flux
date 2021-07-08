package universe

import (
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
)

const PredictLinearKind = "predictLinear"

type PredictLinearOpSpec struct {
	ValueDst    string   `json:"valueDst"`
	WantedValue float64  `json:"wantedValue"`
	Columns     []string `json:"column"`
}

func init() {
	predictLinearSignature := execute.AggregateSignature(map[string]semantic.PolyType{
		"valueDst":    semantic.String,
		"wantedValue": semantic.Float,
		"columns":  semantic.NewArrayPolyType(semantic.String),
	}, []string{"columns"})

	flux.RegisterPackageValue("universe", PredictLinearKind, flux.FunctionValue(PredictLinearKind, createPredictLinearOpSpec, predictLinearSignature))
	flux.RegisterOpSpec(PredictLinearKind, newPredictLinearOp)
	plan.RegisterProcedureSpec(PredictLinearKind, newPredictLinearProcedure, PredictLinearKind)
	execute.RegisterTransformation(PredictLinearKind, createPredictLinearTransformation)
}

func createPredictLinearOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(PredictLinearOpSpec)

	label, ok, err := args.GetString("valueDst")
	if err != nil {
		return nil, err
	} else if ok {
		spec.ValueDst = label
	} else {
		spec.ValueDst = execute.DefaultTimeColLabel
	}

	wantedValue, ok, err := args.GetFloat("wantedValue")
	if err != nil {
		return nil, err
	} else if ok {
		spec.WantedValue = wantedValue
	} else {
		return nil, errors.New(codes.Internal, "must provide 'wantedValue' argument")
	}

	if cols, ok, err := args.GetArray("columns", semantic.String); err != nil {
		return nil, err
	} else if ok {
		columns, err := interpreter.ToStringArray(cols)
		if err != nil {
			return nil, err
		}
		spec.Columns = columns
	} else {
		spec.Columns = []string{execute.DefaultValueColLabel, execute.DefaultTimeColLabel}
	}
	if len(spec.Columns) != 2 {
		return nil, errors.New(codes.Internal, "must provide exactly two columns")
	}
	return spec, nil
}

func newPredictLinearOp() flux.OperationSpec {
	return new(PredictLinearOpSpec)
}

func (s *PredictLinearOpSpec) Kind() flux.OperationKind {
	return PredictLinearKind
}

type PredictLinearProcedureSpec struct {
	ValueLabel  string
	WantedValue float64
	Columns            []string
}

func newPredictLinearProcedure(qs flux.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*PredictLinearOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	ps := &PredictLinearProcedureSpec{
		ValueLabel:  spec.ValueDst,
		WantedValue: spec.WantedValue,
	}
	ps.Columns = make([]string, len(spec.Columns))
	copy(ps.Columns, spec.Columns)

	return ps, nil
}

func (s *PredictLinearProcedureSpec) Kind() plan.ProcedureKind {
	return PredictLinearKind
}

func (s *PredictLinearProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(PredictLinearProcedureSpec)
	*ns = *s

	if s.Columns != nil {
		ns.Columns = make([]string, len(s.Columns))
		copy(ns.Columns, s.Columns)
	}

	return ns
}

type PredictLinearTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache
	spec  PredictLinearProcedureSpec

	n,
	symX,
	symY,
	symXY,
	symX2 float64
}

func createPredictLinearTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*PredictLinearProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewPredictLinearTransformation(d, cache, s)
	return t, d, nil
}

func NewPredictLinearTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *PredictLinearProcedureSpec) *PredictLinearTransformation {
	return &PredictLinearTransformation{
		d:     d,
		cache: cache,
		spec:  *spec,
	}
}

func (t *PredictLinearTransformation) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	return t.d.RetractTable(key)
}

func (t *PredictLinearTransformation) Process(id execute.DatasetID, tbl flux.Table) error {
	cols := tbl.Cols()
	builder, created := t.cache.TableBuilder(tbl.Key())
	if !created {
		return fmt.Errorf("predictLinear found duplicate table with key: %v", tbl.Key())
	}

	if err := execute.AddTableKeyCols(tbl.Key(), builder); err != nil {
		return errors.Wrap(err, codes.Internal,"failed to add table cols")
	}

	valueIdx, err := builder.AddCol(flux.ColMeta{
		Label: t.spec.ValueLabel,
		Type:  flux.TTime,
	})

	if err != nil {
		return errors.Wrap(err, codes.Internal, "failed to add time col")
	}

	valueIdy, err := builder.AddCol(flux.ColMeta{
		Label: execute.DefaultValueColLabel,
		Type:  flux.TFloat,
	})

	if err != nil {
		return errors.Wrap(err, codes.Internal, "failed to add value col")
	}

	yIdx := execute.ColIdx(t.spec.Columns[0], cols)
	xIdx := execute.ColIdx(t.spec.Columns[1], cols)

	if cols[xIdx].Type != flux.TTime {
		return errors.New(codes.Internal, "Last column provided for linearPredict should be of type Time")
	}

	t.reset()
	err = tbl.Do(func(cr flux.ColReader) error {
		switch typ := cols[yIdx].Type; typ {
		case flux.TFloat:
			t.DoFloat(cr.Floats(yIdx), cr.Times(xIdx))
		case flux.TInt:
			t.DoInt(cr.Ints(yIdx), cr.Times(xIdx))
		default:
			return fmt.Errorf("predictLinear does not support %v", typ)
		}
		return nil
	})
	if err != nil {
		return err
	}

	value, err := t.value()
	if err != nil {
		return nil
	}

	if err := execute.AppendKeyValues(tbl.Key(), builder); err != nil {
		return errors.Wrap(err, codes.Internal,"failed to append key values")
	}

	if err := builder.AppendTime(valueIdx, value); err != nil {
		return errors.Wrap(err, codes.Internal, "failed to append time")
	}

	if err := builder.AppendFloat(valueIdy, t.spec.WantedValue); err != nil {
		return errors.Wrap(err, codes.Internal, "failed to append float")
	}

	return nil
}

func (t *PredictLinearTransformation) reset() {
	t.n = 0
	t.symX = 0
	t.symY = 0
	t.symXY = 0
	t.symX2 = 0
}

func (t *PredictLinearTransformation) DoFloat(ys *array.Float64, xs *array.Int64) {
	for i := 0; i < xs.Len(); i++ {
		if xs.IsNull(i) || ys.IsNull(i) {
			continue
		}
		x := float64(xs.Value(i))
		y := ys.Value(i)

		t.doFloatGuts(y, x)
	}
}

func (t *PredictLinearTransformation) DoInt(ys *array.Int64, xs *array.Int64) {
	for i := 0; i < xs.Len(); i++ {
		if xs.IsNull(i) || ys.IsNull(i) {
			continue
		}
		y := float64(ys.Value(i))
		x := float64(xs.Value(i))

		t.doFloatGuts(y, x)
	}
}

func (t *PredictLinearTransformation) doFloatGuts(y float64, x float64) {
	t.n++
	t.symX += x
	t.symY += y
	t.symXY += x * y
	t.symX2 += x * x
}

func (t *PredictLinearTransformation) value() (values.Time, error) {
	if t.n < 2 {
		return 0, fmt.Errorf("number of observations should be more than 1")
	}
	covXY := t.symXY - t.symX*t.symY/t.n
	varX := t.symX2 - t.symX*t.symX/t.n

	slope := covXY / varX
	intercept := t.symY/t.n - slope*t.symX/t.n

	// predict at which interval value of interest will fall
	predictTime := values.Time((t.spec.WantedValue - intercept) / slope)

	return predictTime, nil
}

func (t *PredictLinearTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}

func (t *PredictLinearTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}

func (t *PredictLinearTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
