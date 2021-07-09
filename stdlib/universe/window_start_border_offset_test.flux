package universe_test

import "testing"

option now = () => (2020-09-01T15:00:15Z)

inData = "
#datatype,string,long,dateTime:RFC3339,double,string,string,string
#group,false,false,false,false,true,true,true
#default,_result,,,,,,
,result,table,_time,_value,_field,_measurement,host
,,0,2020-09-01T11:53:26Z,63.053321838378906,used_percent,mem,host.local
,,0,2020-09-01T12:09:36Z,62.71536350250244,used_percent,mem,host.local
,,0,2020-09-01T12:55:46Z,62.38760948181152,used_percent,mem,host.local
,,0,2020-09-01T13:09:56Z,62.74595260620117,used_percent,mem,host.local
,,0,2020-09-01T13:57:06Z,62.78183460235596,used_percent,mem,host.local
,,0,2020-09-01T14:10:16Z,62.46745586395264,used_percent,mem,host.local
"

outData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,0,2020-09-01T10:59:15Z,2020-09-01T11:59:15Z,2020-09-01T11:53:26Z,63.053321838378906,used_percent,mem,host.local
,,1,2020-09-01T11:59:15Z,2020-09-01T12:59:15Z,2020-09-01T12:09:36Z,62.71536350250244,used_percent,mem,host.local
,,1,2020-09-01T11:59:15Z,2020-09-01T12:59:15Z,2020-09-01T12:55:46Z,62.38760948181152,used_percent,mem,host.local
,,2,2020-09-01T12:59:15Z,2020-09-01T13:59:15Z,2020-09-01T13:09:56Z,62.74595260620117,used_percent,mem,host.local
,,2,2020-09-01T12:59:15Z,2020-09-01T13:59:15Z,2020-09-01T13:57:06Z,62.78183460235596,used_percent,mem,host.local
,,3,2020-09-01T13:59:15Z,2020-09-01T14:59:15Z,2020-09-01T14:10:16Z,62.46745586395264,used_percent,mem,host.local
"

t_window_start_border_offset = (table=<-) =>
	(table
		|> range(start: 2020-09-01T10:00:00Z)
		|> window(offset: -45000ms, every: 1h))

test _window_start_border_offset = () =>
	({input: testing.loadStorage(csv: inData), want: testing.loadMem(csv: outData), fn: t_window_start_border_offset})