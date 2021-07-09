package ecs_test

import "ecs"
import "csv"
import "testing"

append_test = (name, value) => {
    return ",,0," + name + "," + string(v:value) + "\n"
}

dyn_win1 = ecs.get_dynamic_window_info(int_start: 1569002022,
                                       int_stop:  1569012022,
                                       max: 90,
                                       start_offset: 2,
                                       stop_offset: 2)

dyn_win2 = ecs.get_dynamic_window_info(int_start: 1569001997,
                                       int_stop:  1569112022,
                                       max: 40,
                                       start_offset: 1,
                                       stop_offset: 1)

dyn_dsm1 = ecs.get_dyn_downsample_info(start: 1569001997,
                                       stop:  1569112022,
                                       max: 40,
                                       measurement:"cq_m1",
                                       start_offset: 1,
                                       stop_offset: 1)

dyn_dsm2 = ecs.get_dyn_downsample_info(start: 1567001997,
                                       stop:  1569112022,
                                       max: 200,
                                       measurement:"cq_m2",
                                       start_offset: 2,
                                       stop_offset: 1)



inData = "
#datatype,string,long,string,boolean
#group,false,false,true,false
#default,_result,,,
,result,table,test,value
"
+ append_test(name:"test_no_adding_downsampled", value:
    ecs.get_downsampled_measurement(measurement:"cq_test_nd", period:5,limit:6) == "cq_test_nd")
+ append_test(name:"test_adding_downsampled", value:
    ecs.get_downsampled_measurement(measurement:"cq_test", period:9,limit:6) == "cq_test_downsampled")
+ append_test(name:"minimal_duration_5min", value:
    int(v:dyn_win1.duration) == 300000000000)
+ append_test(name:"larger_duration", value:
    int(v:dyn_win2.duration) == 2700000000000)
+ append_test(name:"start_offset", value:
    dyn_win2.start == 2019-09-20T16:30:00Z)
+ append_test(name:"stop_offset", value:
    dyn_win2.stop == 2019-09-22T00:45:00Z)

// test that downsample is not enabled

+ append_test(name:"dyn_downsample_1", value:
    dyn_dsm1.measurement == "cq_m1")
+ append_test(name:"dyn_downsample_1a", value:
    dyn_dsm1.in_downsample == 0)
+ append_test(name:"dyn_downsample_1_start_offset", value:
    dyn_dsm1.start == 2019-09-20T16:30:00Z)
+ append_test(name:"dyn_downsample_1_stop_offset", value:
    dyn_dsm1.stop == 2019-09-22T00:45:00Z)

// test that downsample is in effect

+ append_test(name:"dyn_downsample_2", value:
    dyn_dsm2.measurement == "cq_m2_downsampled")
+ append_test(name:"dyn_downsample_2a", value:
    dyn_dsm2.in_downsample == 1)
+ append_test(name:"dyn_downsample_2_duration", value:
    int(v:dyn_dsm2.duration) == 86400000000000)
+ append_test(name:"dyn_downsample_2_start_offset", value:
    dyn_dsm2.start == 2019-08-26T00:00:00Z)
+ append_test(name:"dyn_downsample_2_stop_offset", value:
    dyn_dsm2.stop == 2019-09-23T00:00:00Z)



input = csv.from(csv: inData) |> group(columns:["test"])

// copy input stream but make all values true
want = input |> map(fn: (r) => ({value:true}))

// NB sort() is required after input to make streams different
test _ecs_functions = () =>
	({input: input |> map(fn: (r) => ({value:r.value})) |> sort(), want: want, fn: (table=<-) => (table)})
