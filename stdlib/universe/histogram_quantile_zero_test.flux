package universe_test

import "testing"

option now = () => (2030-01-01T00:00:00Z)

inData = "
#datatype,string,long,dateTime:RFC3339,string,double,double
#group,false,false,true,true,false,false
#default,_result,,,,,
,result,table,_time,_field,_value,le
,,0,2018-05-22T19:53:00Z,x_duration_seconds,0,0.0
,,0,2018-05-22T19:53:00Z,x_duration_seconds,0,1.0
,,0,2018-05-22T19:53:00Z,x_duration_seconds,0,4.0
,,0,2018-05-22T19:53:00Z,x_duration_seconds,0,23.0
,,0,2018-05-22T19:53:00Z,x_duration_seconds,0,111.0
,,0,2018-05-22T19:53:00Z,x_duration_seconds,0,537.0
,,0,2018-05-22T19:53:00Z,x_duration_seconds,0,2588.0
,,0,2018-05-22T19:53:00Z,x_duration_seconds,0,12461.0
,,0,2018-05-22T19:53:00Z,x_duration_seconds,0,59999.0
,,0,2018-05-22T19:53:00Z,x_duration_seconds,0,+Inf
"
outData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,double
#group,false,false,true,true,true,true,false
#default,_result,,,,,,
,result,table,_start,_stop,_time,_field,_value
,,0,2018-05-22T19:53:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:00Z,x_duration_seconds,0.0
"

t_histogram_quantile = (table=<-) =>
	(table
		|> range(start: 2018-05-22T19:53:00Z)
        |> histogramQuantile(quantile:0.25))

test _histogram_quantile_zero = () =>
	({input: testing.loadStorage(csv: inData), want: testing.loadMem(csv: outData), fn: t_histogram_quantile})