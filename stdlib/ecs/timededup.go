package ecs

import (
	"fmt"
	"github.com/gusakk/flux"
	"github.com/gusakk/flux/codes"
	"github.com/gusakk/flux/execute"
	"github.com/gusakk/flux/internal/errors"
	"github.com/gusakk/flux/plan"
	"github.com/gusakk/flux/semantic"
)

const TimeDedupKind = "timededup"

type TimeDedupOpSpec struct {
	Target  string `json:"target"`
	Compare string `json:"compare"`
}

func init() {
	timeDedupSignature := flux.FunctionSignature(
		map[string]semantic.PolyType{
			"target":  semantic.String,
			"compare": semantic.String,
		},
		nil,
	)

	flux.RegisterPackageValue("ecs", TimeDedupKind, flux.FunctionValue(TimeDedupKind, createTimeDedupOpSpec, timeDedupSignature))
	flux.RegisterOpSpec(TimeDedupKind, newTimeDedupOp)
	plan.RegisterProcedureSpec(TimeDedupKind, newDupFilterProcedure, TimeDedupKind)
	execute.RegisterTransformation(TimeDedupKind, createTimeDedupTransformation)
}

func createTimeDedupOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(TimeDedupOpSpec)

	target, ok, err := args.GetString("target")
	switch {
	case err != nil:
		return nil, err
	case ok:
		spec.Target = target
	default:
		spec.Target = execute.DefaultTimeColLabel
	}
	compare, ok, err := args.GetString("compare")
	switch {
	case err != nil:
		return nil, err
	case ok:
		spec.Compare = compare
	default:
		spec.Compare = execute.DefaultValueColLabel
	}

	return spec, nil
}

func newTimeDedupOp() flux.OperationSpec {
	return new(TimeDedupOpSpec)
}

func (s *TimeDedupOpSpec) Kind() flux.OperationKind {
	return TimeDedupKind
}

type TimeDedupProcedureSpec struct {
	plan.DefaultCost
	Target  string
	Compare string
}

func newDupFilterProcedure(qs flux.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*TimeDedupOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &TimeDedupProcedureSpec{
		Target:  spec.Target,
		Compare: spec.Compare,
	}, nil
}

func (s *TimeDedupProcedureSpec) Kind() plan.ProcedureKind {
	return TimeDedupKind
}
func (s *TimeDedupProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(TimeDedupProcedureSpec)
	ns.Target = s.Target
	ns.Compare = s.Compare
	return ns
}

// TriggerSpec implements plan.TriggerAwareProcedureSpec
func (s *TimeDedupProcedureSpec) TriggerSpec() plan.TriggerSpec {
	return plan.NarrowTransformationTriggerSpec{}
}

func createTimeDedupTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*TimeDedupProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewTimeDedupTransformation(d, cache, s)
	return t, d, nil
}

type timeDedupTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache

	target  string
	compare string
}

func NewTimeDedupTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *TimeDedupProcedureSpec) *timeDedupTransformation {
	return &timeDedupTransformation{
		d:       d,
		cache:   cache,
		target:  spec.Target,
		compare: spec.Compare,
	}
}

func (t *timeDedupTransformation) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	return t.d.RetractTable(key)
}

func (t *timeDedupTransformation) Process(id execute.DatasetID, tbl flux.Table) error {
	key := tbl.Key()

	builder, created := t.cache.TableBuilder(key)
	if !created {
		return fmt.Errorf("dedup found duplicate table with key: %v", key)
	}
	if err := execute.AddTableCols(tbl, builder); err != nil {
		return err
	}

	err := tbl.Do(func(cr flux.ColReader) error {
		timeDedup := make(map[int64]int)
		l := cr.Len()
		tcolIdx := execute.ColIdx(t.target, builder.Cols())

		if tcolIdx < 0 {
			return fmt.Errorf("no column %q exists", t.target)
		}
		vcolIdx := execute.ColIdx(t.compare, builder.Cols())
		if vcolIdx < 0 {
			return fmt.Errorf("no column %q exists", t.compare)
		}
		//check type of columns
		timeCol := builder.Cols()[tcolIdx]
		valCol := builder.Cols()[vcolIdx]
		if timeCol.Type != flux.TTime || valCol.Type != flux.TFloat {
			return fmt.Errorf("wrong type of columns %s:%v,%s:%v ", t.target, timeCol.Type, t.compare, valCol.Type)
		}
		// flag to discover duplicates
		var duplicateFoundFlag bool
		// loop over the records to find duplicates
		for i := 0; i < l; i++ {
			tm := cr.Times(tcolIdx).Value(i)
			if ii, ok := timeDedup[tm]; ok {
				duplicateFoundFlag = true
				// get current and previous value
				val, prevVal := cr.Floats(vcolIdx).Value(i), cr.Floats(vcolIdx).Value(ii)
				if val > prevVal {
					timeDedup[tm] = i
				}
			} else {
				timeDedup[tm] = i
			}
		}

		if !duplicateFoundFlag {
			// just pass all records without a change
			for i := 0; i < l; i++ {
				if err := execute.AppendRecord(i, cr, builder); err != nil {
					return err
				}
			}
			return nil

		}

		for _, i := range timeDedup {
			if err := execute.AppendRecord(i, cr, builder); err != nil {
				return err
			}
		}
		colsToSort := make([]string, 1)
		colsToSort[0] = t.target
		builder.Sort(colsToSort, false)

		return nil
	})

	if err != nil {
		return errors.Wrap(err, codes.Internal, "failed to apply timededup transformation")
	}
	return nil
}

func (t *timeDedupTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *timeDedupTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *timeDedupTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
