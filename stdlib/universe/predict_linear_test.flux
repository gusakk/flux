package universe_test

import "testing"

option now = () => (2030-01-01T00:00:00Z)

inData = "
#datatype,string,long,dateTime:RFC3339,double
#group,false,false,false,false
#default,_result,,,
,result,table,_time,_value
,,0,2018-08-10T09:30:00Z,1
,,0,2018-08-11T09:30:00Z,2
,,0,2018-08-12T09:30:00Z,3
,,0,2018-08-13T09:30:00Z,4
,,0,2018-08-14T09:30:00Z,5
,,0,2018-08-15T09:30:00Z,6
"

outData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double
#group,false,false,true,true,false,false
#default,_result,,,,,
,result,table,_start,_stop,_time,_value
,,0,2018-08-10T09:30:00Z,2030-01-01T00:00:00Z,2018-08-19T09:30:00.011552Z,10
"

t_predictLinear = (table=<-) =>
	(table
		|> range(start: 2018-08-10T09:30:00.00Z)
		|> predictLinear(wantedValue: 10.0))

test _predictLinear = () =>
	({input: testing.loadStorage(csv: inData), want: testing.loadMem(csv: outData), fn: t_predictLinear})

