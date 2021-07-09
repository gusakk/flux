package universe

import (
	"fmt"
	"github.com/gusakk/flux"
	"github.com/gusakk/flux/execute"
	"github.com/gusakk/flux/plan"
	"github.com/gusakk/flux/semantic"
	"github.com/gusakk/flux/values"
)

const (
	FillMissingKind = "fillMissing"
	TimeColLabel    = execute.DefaultTimeColLabel
	StopColLabel    = execute.DefaultStopColLabel
)

// The fillMissing() function adds new points with provided period if they are missing.
// Value is taken from previous value.
// Points are added until time from _stop column (exclusively).
type FillMissingOpSpec struct {
	Period flux.Duration `json:"period"`
}

func init() {
	fillMissingSignature := flux.FunctionSignature(
		map[string]semantic.PolyType{
			"period": semantic.Duration,
		},
		[]string{"period"},
	)

	flux.RegisterPackageValue("universe", FillMissingKind, flux.FunctionValue(FillMissingKind, createFillMissingOpSpec, fillMissingSignature))
	flux.RegisterOpSpec(FillMissingKind, newFillMissingOp)
	plan.RegisterProcedureSpec(FillMissingKind, newFillMissingProcedure, FillMissingKind)
	execute.RegisterTransformation(FillMissingKind, createFillMissingTransformation)
}

func createFillMissingOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(FillMissingOpSpec)

	period, err := args.GetRequiredDuration("period")
	if err != nil {
		return nil, err
	}
	spec.Period = period

	return spec, nil
}

func newFillMissingOp() flux.OperationSpec {
	return new(FillMissingOpSpec)
}

func (s *FillMissingOpSpec) Kind() flux.OperationKind {
	return FillMissingKind
}

type FillMissingProcedureSpec struct {
	plan.DefaultCost
	Period flux.Duration
}

func newFillMissingProcedure(qs flux.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*FillMissingOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &FillMissingProcedureSpec{Period: spec.Period}, nil
}

func (s *FillMissingProcedureSpec) Kind() plan.ProcedureKind {
	return FillMissingKind
}

func (s *FillMissingProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(FillMissingProcedureSpec)
	*ns = *s
	return ns
}

func createFillMissingTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*FillMissingProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewFillMissingTransformation(d, cache, s)
	return t, d, nil
}

type fillMissingTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache

	period flux.Duration
}

func NewFillMissingTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *FillMissingProcedureSpec) *fillMissingTransformation {
	return &fillMissingTransformation{
		d:      d,
		cache:  cache,
		period: spec.Period,
	}
}

func (t *fillMissingTransformation) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	return t.d.RetractTable(key)
}

func (t *fillMissingTransformation) Process(id execute.DatasetID, tbl flux.Table) error {
	builder, created := t.cache.TableBuilder(tbl.Key())
	if !created {
		return fmt.Errorf("fillMissing found duplicate table with key: %v", tbl.Key())
	}
	if err := execute.AddTableCols(tbl, builder); err != nil {
		return err
	}

	timeColIdx := execute.ColIdx(TimeColLabel, tbl.Cols())
	if timeColIdx < 0 {
		return fmt.Errorf("fillMissing: missing time column %q", TimeColLabel)
	}

	stopColIdx := execute.ColIdx(StopColLabel, tbl.Cols())
	if stopColIdx < 0 {
		return fmt.Errorf("fillMissing: missing stop column %q", StopColLabel)
	}

	return tbl.Do(func(cr flux.ColReader) error {
		inputLen := cr.Len()

		for i := 0; i < inputLen; i++ {
			if err := execute.AppendRecord(i, cr, builder); err != nil {
				return err
			}
			currentTime := execute.ValueForRow(cr, i, timeColIdx).Time()

			var fillUntil values.Time
			if i < inputLen-1 {
				// fill until time of the next record
				fillUntil = execute.ValueForRow(cr, i+1, timeColIdx).Time()
			} else {
				// fill until _stop value of the last record
				fillUntil = execute.ValueForRow(cr, inputLen-1, stopColIdx).Time()
			}

			for {
				currentTime = currentTime.Add(values.Duration(t.period))
				if currentTime >= fillUntil {
					break
				}

				for j := range builder.Cols() {
					var value values.Value
					if j == timeColIdx {
						value = values.New(currentTime)
					} else {
						value = execute.ValueForRow(cr, i, j)
					}
					if err := builder.AppendValue(j, value); err != nil {
						return err
					}
				}
			}
		}

		return nil
	})
}

func (t *fillMissingTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}

func (t *fillMissingTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateWatermark(pt)
}

func (t *fillMissingTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
