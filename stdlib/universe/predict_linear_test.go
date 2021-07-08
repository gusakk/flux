package universe_test

import (
	"github.com/gusakk/flux"
	"github.com/gusakk/flux/execute"
	"github.com/gusakk/flux/execute/executetest"
	"github.com/gusakk/flux/querytest"
	"github.com/gusakk/flux/stdlib/influxdata/influxdb"
	"github.com/gusakk/flux/stdlib/universe"
	"testing"
)

func TestPredictLinearOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"predictLinear","kind":"predictLinear"}`)
	op := &flux.Operation{
		ID:   "predictLinear",
		Spec: &universe.PredictLinearOpSpec{},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestPredictLinear_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "simple regression",
			Raw:  `from(bucket:"mydb") |> predictLinear(columns:["a","b"], wantedValue: 10.0)`,
			Want: &flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "from0",
						Spec: &influxdb.FromOpSpec{
							Bucket: "mydb",
						},
					},
					{
						ID: "predictLinear1",
						Spec: &universe.PredictLinearOpSpec{
							ValueDst:    execute.DefaultTimeColLabel,
							WantedValue: 10.0,
							Columns:     []string{"a", "b"},
						},
					},
				},
				Edges: []flux.Edge{
					{Parent: "from0", Child: "predictLinear1"},
				},
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			querytest.NewQueryTestHelper(t, tc)
		})
	}
}

func TestPredictLinear_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *universe.PredictLinearProcedureSpec
		data []flux.Table
		want []*executetest.Table
	}{
		{
			name: "simple regression",
			spec: &universe.PredictLinearProcedureSpec{
				WantedValue: 50,
				ValueLabel:  execute.DefaultTimeColLabel,
				Columns:     []string{"x", "_time"},
			},
			data: []flux.Table{&executetest.Table{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "x", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(0), 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(1), 2.0},
					{execute.Time(0), execute.Time(5), execute.Time(2), 3.0},
					{execute.Time(0), execute.Time(5), execute.Time(3), 4.0},
					{execute.Time(0), execute.Time(5), execute.Time(4), 5.0},
				},
			}},
			want: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(49), 50.0},
				},
			}},
		},
		{
			name: "earlier time",
			spec: &universe.PredictLinearProcedureSpec{
				WantedValue: 0,
				ValueLabel:  execute.DefaultTimeColLabel,
				Columns:     []string{"x", "_time"},
			},
			data: []flux.Table{&executetest.Table{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "x", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(10), 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(11), 2.0},
					{execute.Time(0), execute.Time(5), execute.Time(12), 3.0},
					{execute.Time(0), execute.Time(5), execute.Time(13), 4.0},
					{execute.Time(0), execute.Time(5), execute.Time(14), 5.0},
				},
			}},
			want: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(9), 0.0},
				},
			}},
		},
		{
			name: "negative time",
			spec: &universe.PredictLinearProcedureSpec{
				WantedValue: 0,
				ValueLabel:  execute.DefaultTimeColLabel,
				Columns:     []string{"x", "_time"},
			},
			data: []flux.Table{&executetest.Table{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "x", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(0), 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(1), 2.0},
					{execute.Time(0), execute.Time(5), execute.Time(2), 3.0},
					{execute.Time(0), execute.Time(5), execute.Time(3), 4.0},
					{execute.Time(0), execute.Time(5), execute.Time(4), 5.0},
				},
			}},
			want: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(-1), 0.0},
				},
			}},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want,
				nil,
				func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
					return universe.NewPredictLinearTransformation(d, c, tc.spec)
				},
			)
		})
	}
}
