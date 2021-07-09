package universe_test

import (
	"fmt"
	"github.com/gusakk/flux"
	"github.com/gusakk/flux/execute"
	"github.com/gusakk/flux/execute/executetest"
	"github.com/gusakk/flux/querytest"
	"github.com/gusakk/flux/stdlib/influxdata/influxdb"
	"github.com/gusakk/flux/stdlib/universe"
	"testing"
	"time"
)

func TestFillMissingOperationMarshaling(t *testing.T) {
	data := []byte(`{"id":"fillMissing","kind":"fillMissing","spec":{"period":"1s"}}`)
	op := &flux.Operation{
		ID: "fillMissing",
		Spec: &universe.FillMissingOpSpec{
			Period: flux.ConvertDuration(time.Second),
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestFillMissingNewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "from with range and fillMissing",
			Raw:  `from(bucket: "mydb") |> range(start: -4h, stop: -2h) |> fillMissing(period: 5m)`,
			Want: &flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "from0",
						Spec: &influxdb.FromOpSpec{
							Bucket: "mydb",
						},
					},
					{
						ID: "range1",
						Spec: &universe.RangeOpSpec{
							Start: flux.Time{
								Relative:   -4 * time.Hour,
								IsRelative: true,
							},
							Stop: flux.Time{
								Relative:   -2 * time.Hour,
								IsRelative: true,
							},
							TimeColumn:  "_time",
							StartColumn: "_start",
							StopColumn:  "_stop",
						},
					},
					{
						ID: "fillMissing2",
						Spec: &universe.FillMissingOpSpec{
							Period: flux.ConvertDuration(5 * time.Minute),
						},
					},
				},
				Edges: []flux.Edge{
					{Parent: "from0", Child: "range1"},
					{Parent: "range1", Child: "fillMissing2"},
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

func TestFillMissingProcess(t *testing.T) {
	start := execute.Time(time.Date(2017, 10, 10, 10, 0, 0, 0, time.UTC).UnixNano())
	testCases := []struct {
		name    string
		spec    *universe.FillMissingProcedureSpec
		data    []flux.Table
		want    []*executetest.Table
		wantErr error
	}{
		{
			name: "no _time error",
			spec: &universe.FillMissingProcedureSpec{
				Period: flux.ConvertDuration(time.Minute),
			},
			data: []flux.Table{
				&executetest.Table{
					ColMeta: []flux.ColMeta{
						{Label: "_stop", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{start + execute.Time(2*time.Minute+time.Second), 2.0},
						{start + execute.Time(2*time.Minute+time.Second), 1.0},
					},
				},
			},
			wantErr: fmt.Errorf("fillMissing: missing time column \"_time\""),
		},
		{
			name: "no _stop error",
			spec: &universe.FillMissingProcedureSpec{
				Period: flux.ConvertDuration(time.Minute),
			},
			data: []flux.Table{
				&executetest.Table{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{start + execute.Time(2*time.Minute), 2.0},
						{start + execute.Time(3*time.Minute), 1.0},
					},
				},
			},
			wantErr: fmt.Errorf("fillMissing: missing stop column \"_stop\""),
		},
		{
			name: "no fill",
			spec: &universe.FillMissingProcedureSpec{
				Period: flux.ConvertDuration(time.Minute),
			},
			data: []flux.Table{
				&executetest.Table{
					ColMeta: []flux.ColMeta{
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{start + execute.Time(2*time.Minute+time.Second), start + execute.Time(1*time.Minute), 2.0},
						{start + execute.Time(2*time.Minute+time.Second), start + execute.Time(2*time.Minute), 1.0},
					},
				},
			},
			want: []*executetest.Table{{
				ColMeta: []flux.ColMeta{
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{start + execute.Time(2*time.Minute+time.Second), start + execute.Time(1*time.Minute), 2.0},
					{start + execute.Time(2*time.Minute+time.Second), start + execute.Time(2*time.Minute), 1.0},
				},
			}},
		},
		{
			name: "no fill last time equals stop",
			spec: &universe.FillMissingProcedureSpec{
				Period: flux.ConvertDuration(time.Minute),
			},
			data: []flux.Table{
				&executetest.Table{
					ColMeta: []flux.ColMeta{
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{start + execute.Time(2*time.Minute), start + execute.Time(1*time.Minute), 2.0},
						{start + execute.Time(2*time.Minute), start + execute.Time(2*time.Minute), 1.0},
					},
				},
			},
			want: []*executetest.Table{{
				ColMeta: []flux.ColMeta{
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{start + execute.Time(2*time.Minute), start + execute.Time(1*time.Minute), 2.0},
					{start + execute.Time(2*time.Minute), start + execute.Time(2*time.Minute), 1.0},
				},
			}},
		},
		{
			name: "no fill single item",
			spec: &universe.FillMissingProcedureSpec{
				Period: flux.ConvertDuration(time.Minute),
			},
			data: []flux.Table{
				&executetest.Table{
					ColMeta: []flux.ColMeta{
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{start + execute.Time(2*time.Minute+time.Second), start + execute.Time(2*time.Minute), 1.0},
					},
				},
			},
			want: []*executetest.Table{{
				ColMeta: []flux.ColMeta{
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{start + execute.Time(2*time.Minute+time.Second), start + execute.Time(2*time.Minute), 1.0},
				},
			}},
		},
		{
			name: "fill after last item",
			spec: &universe.FillMissingProcedureSpec{
				Period: flux.ConvertDuration(time.Second),
			},
			data: []flux.Table{
				&executetest.Table{
					ColMeta: []flux.ColMeta{
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{start + execute.Time(3*time.Second+time.Millisecond), start + execute.Time(time.Second), 5.0},
					},
				},
			},
			want: []*executetest.Table{
				{
					ColMeta: []flux.ColMeta{
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{start + execute.Time(3*time.Second+time.Millisecond), start + execute.Time(time.Second), 5.0},
						{start + execute.Time(3*time.Second+time.Millisecond), start + execute.Time(2*time.Second), 5.0},
						{start + execute.Time(3*time.Second+time.Millisecond), start + execute.Time(3*time.Second), 5.0},
					},
				},
			},
		},
		{
			name: "fill after last item stop excluded",
			spec: &universe.FillMissingProcedureSpec{
				Period: flux.ConvertDuration(time.Second),
			},
			data: []flux.Table{
				&executetest.Table{
					ColMeta: []flux.ColMeta{
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{start + execute.Time(3*time.Second), start + execute.Time(time.Second), 5.0},
					},
				},
			},
			want: []*executetest.Table{
				{
					ColMeta: []flux.ColMeta{
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{start + execute.Time(3*time.Second), start + execute.Time(time.Second), 5.0},
						{start + execute.Time(3*time.Second), start + execute.Time(2*time.Second), 5.0},
					},
				},
			},
		},
		{
			name: "fill between and after",
			spec: &universe.FillMissingProcedureSpec{
				Period: flux.ConvertDuration(time.Second),
			},
			data: []flux.Table{
				&executetest.Table{
					ColMeta: []flux.ColMeta{
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(2*time.Second), 5.0},
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(5*time.Second), 3.0},
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(6*time.Second), 2.0},
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(8*time.Second), 1.0},
					},
				},
			},
			want: []*executetest.Table{
				{
					ColMeta: []flux.ColMeta{
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(2*time.Second), 5.0},
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(3*time.Second), 5.0},
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(4*time.Second), 5.0},
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(5*time.Second), 3.0},
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(6*time.Second), 2.0},
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(7*time.Second), 2.0},
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(8*time.Second), 1.0},
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(9*time.Second), 1.0},
						{start + execute.Time(10*time.Second+time.Millisecond), start + execute.Time(10*time.Second), 1.0},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.ProcessTestHelper(t, tc.data, tc.want, tc.wantErr,
				func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
					return universe.NewFillMissingTransformation(d, c, tc.spec)
				})
		})
	}
}
