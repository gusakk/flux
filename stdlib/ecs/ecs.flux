package ecs

builtin ecs_package_version
builtin timededup

// convert name of measurement to _downsampled
get_downsampled_measurement = (measurement, ds_suffix="_downsampled", period, limit) => {
    return measurement + ["",ds_suffix][int(v:period >= limit)]
}

// shortcut to calculate dynamic window based on maximum desired windows per view
get_dynamic_window_info = (int_start, int_stop, max, start_offset=0, stop_offset=2, min_window=300) => {
    time_int = int_stop - int_start
    sec_in_ns = 1000000000
    window_len = time_int / max / min_window * min_window
    is_min_window = window_len < min_window
    window_len = (int(v:(is_min_window)) * min_window) + (int(v:(not is_min_window)) * window_len)
    wl_dur = duration(v:window_len * sec_in_ns)
    start_dt = time(v:((int_start/window_len) - start_offset) * window_len * sec_in_ns)
    end_dt = time(v:((int_stop/window_len) + stop_offset) * window_len * sec_in_ns)
   return {duration: wl_dur, interval: time_int, start: start_dt, stop: end_dt}
}

// max - maximum number of points per graph
// downsample_limit - time to switch to downsample (default=5 days)
// min_downsample_window - minimal window in downsample mode (default=1 day)
get_dyn_downsample_info = (start, stop, max,
                           start_offset=2, stop_offset=1,
                           measurement,
                           ds_suffix="_downsampled",
                           downsample_limit=432000,
                           min_downsample_window=86400
    ) => {
    int_start = int(v:start)
    int_stop = int(v:stop)
    time_int = int_stop - int_start
    in_ds = int(v:time_int >= downsample_limit)
    new_measurement = measurement + ["",ds_suffix][in_ds]
    new_window = [300, min_downsample_window][in_ds]
    dinf = get_dynamic_window_info(int_start:int_start,
                                   int_stop:int_stop,
                                   max:max, start_offset:start_offset,
                                   stop_offset:stop_offset,
                                   min_window:new_window)
    return {
            in_downsample: in_ds,
            duration: dinf.duration,
            start: dinf.start, stop: dinf.stop,
            measurement: new_measurement
    }
}

//sample:
//inf = ecs.get_dyn_downsample_info(start:$__range_from_s,
//stop:$__range_to_s,
//max:200,
//measurement:"cq_performance_transaction_ns"
//)