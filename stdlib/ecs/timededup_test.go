package ecs_test

import (
	"github.com/gusakk/flux"
	"github.com/gusakk/flux/execute"
	"github.com/gusakk/flux/execute/executetest"
	"github.com/gusakk/flux/stdlib/ecs"
	"math/rand"
	"testing"
	"time"
)

func Test_timededupTransformation_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *ecs.TimeDedupProcedureSpec
		data []flux.Table
		want []*executetest.Table
	}{
		{
			name: "No duplicates by time",
			spec: &ecs.TimeDedupProcedureSpec{
				Target:  "_time",
				Compare: "_value",
			},
			data: []flux.Table{&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), 2.0},
					{execute.Time(1), 1.0},
					{execute.Time(2), 3.0},
					{execute.Time(3), 4.0},
					{execute.Time(4), 2.0},
					{execute.Time(5), 6.0},
					{execute.Time(6), 2.0},
					{execute.Time(7), 7.0},
					{execute.Time(8), 3.0},
					{execute.Time(9), 8.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), 2.0},
					{execute.Time(1), 1.0},
					{execute.Time(2), 3.0},
					{execute.Time(3), 4.0},
					{execute.Time(4), 2.0},
					{execute.Time(5), 6.0},
					{execute.Time(6), 2.0},
					{execute.Time(7), 7.0},
					{execute.Time(8), 3.0},
					{execute.Time(9), 8.0},
				},
			}},
		},
		{
			name: "1 duplicate",
			spec: &ecs.TimeDedupProcedureSpec{
				Target:  "_time",
				Compare: "_value",
			},
			data: []flux.Table{&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), 2.0},
					{execute.Time(1), 1.0},
					{execute.Time(2), 3.0},
					{execute.Time(3), 4.0},
					{execute.Time(4), 2.0},
					{execute.Time(5), 6.0},
					{execute.Time(6), 2.0},
					{execute.Time(7), 7.0},
					{execute.Time(8), 3.0},
					{execute.Time(9), 8.0},
					{execute.Time(1), 10.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), 2.0},
					{execute.Time(1), 10.0},
					{execute.Time(2), 3.0},
					{execute.Time(3), 4.0},
					{execute.Time(4), 2.0},
					{execute.Time(5), 6.0},
					{execute.Time(6), 2.0},
					{execute.Time(7), 7.0},
					{execute.Time(8), 3.0},
					{execute.Time(9), 8.0},
				},
			}},
		},
		{
			name: "A lot of duplicates",
			spec: &ecs.TimeDedupProcedureSpec{
				Target:  "_time",
				Compare: "_value",
			},
			data: []flux.Table{&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), 2.0},
					{execute.Time(1), 1.0},
					{execute.Time(2), 3.0},
					{execute.Time(3), 4.0},
					{execute.Time(4), 2.0},
					{execute.Time(5), 6.0},
					{execute.Time(6), 2.0},
					{execute.Time(7), 7.0},
					{execute.Time(8), 3.0},
					{execute.Time(9), 8.0},
					{execute.Time(0), 2.0},
					{execute.Time(1), 10.0},
					{execute.Time(2), 3.0},
					{execute.Time(3), 40.0},
					{execute.Time(4), 2.0},
					{execute.Time(5), 60.0},
					{execute.Time(6), 2.0},
					{execute.Time(7), 70.0},
					{execute.Time(8), 3.0},
					{execute.Time(9), 80.0},
					{execute.Time(9), 80.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), 2.0},
					{execute.Time(1), 10.0},
					{execute.Time(2), 3.0},
					{execute.Time(3), 40.0},
					{execute.Time(4), 2.0},
					{execute.Time(5), 60.0},
					{execute.Time(6), 2.0},
					{execute.Time(7), 70.0},
					{execute.Time(8), 3.0},
					{execute.Time(9), 80.0},
				},
			}},
		},
		{
			name: "A lot of duplicates diff order",
			spec: &ecs.TimeDedupProcedureSpec{
				Target:  "_time",
				Compare: "_value",
			},
			data: []flux.Table{&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), 2.0},
					{execute.Time(1), 1.0},
					{execute.Time(1), 10.0},
					{execute.Time(2), 3.0},
					{execute.Time(3), 4.0},
					{execute.Time(4), 2.0},
					{execute.Time(5), 6.0},
					{execute.Time(6), 2.0},
					{execute.Time(7), 7.0},
					{execute.Time(8), 3.0},
					{execute.Time(9), 8.0},
					{execute.Time(0), 2.0},
					{execute.Time(2), 3.0},
					{execute.Time(3), 40.0},
					{execute.Time(4), 2.0},
					{execute.Time(5), 60.0},
					{execute.Time(6), 2.0},
					{execute.Time(7), 70.0},
					{execute.Time(8), 3.0},
					{execute.Time(9), 80.0},
					{execute.Time(9), 80.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), 2.0},
					{execute.Time(1), 10.0},
					{execute.Time(2), 3.0},
					{execute.Time(3), 40.0},
					{execute.Time(4), 2.0},
					{execute.Time(5), 60.0},
					{execute.Time(6), 2.0},
					{execute.Time(7), 70.0},
					{execute.Time(8), 3.0},
					{execute.Time(9), 80.0},
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
					return ecs.NewTimeDedupTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

// generate data with duplicates
func genDuplicateData(count, dupCoutnt int) (expected, generated [][]interface{}) {
	for i := 0; i < count; i++ {
		expected = append(expected, []interface{}{execute.Time(1000000 + i), 1000000.0 + float64(i)})
	}
	for i := 0; i < dupCoutnt; i++ {
		for ii := range expected {
			generated = append(generated, []interface{}{expected[ii][0], expected[ii][1].(float64) - float64(i)})
		}
	}
	return
}

func Test_timededupTransformation_Process_generated(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	e1000010, g1000010 := genDuplicateData(10000, 10)
	rand.Shuffle(len(g1000010), func(i, j int) { g1000010[i], g1000010[j] = g1000010[j], g1000010[i] })
	e5000100, g5000100 := genDuplicateData(5000, 100)
	rand.Shuffle(len(g5000100), func(i, j int) { g5000100[i], g5000100[j] = g5000100[j], g5000100[i] })
	e100500, g100500 := genDuplicateData(100, 500)
	rand.Shuffle(len(g100500), func(i, j int) { g100500[i], g100500[j] = g100500[j], g100500[i] })
	testCases := []struct {
		name string
		spec *ecs.TimeDedupProcedureSpec
		data []flux.Table
		want []*executetest.Table
	}{
		{
			name: "Shuffled duplicate results 10000 10",
			spec: &ecs.TimeDedupProcedureSpec{
				Target:  "_time",
				Compare: "_value",
			},
			data: []flux.Table{&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: g1000010,
			}},
			want: []*executetest.Table{{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: e1000010,
			}},
		},
		{
			name: "Shuffled duplicate results 5000 100",
			spec: &ecs.TimeDedupProcedureSpec{
				Target:  "_time",
				Compare: "_value",
			},
			data: []flux.Table{&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: g5000100,
			}},
			want: []*executetest.Table{{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: e5000100,
			}},
		},
		{
			name: "Shuffled duplicate results 100 500",
			spec: &ecs.TimeDedupProcedureSpec{
				Target:  "_time",
				Compare: "_value",
			},
			data: []flux.Table{&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: g100500,
			}},
			want: []*executetest.Table{{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: e100500,
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
					return ecs.NewTimeDedupTransformation(d, c, tc.spec)
				},
			)
		})
	}
}
