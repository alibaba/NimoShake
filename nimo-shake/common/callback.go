package utils

import "time"

/*
 * retry the callback function until successfully or overpass the threshold.
 * @input:
 *     times: retry times
 *     sleep: sleep time by ms interval
 *     cb: callback
 * the callback should return true means need retry.
 */
func CallbackRetry(times int, sleep int, cb func() bool) bool {
	for i := 0; i < times; i++ {
		if cb() == false { // callback, true means retry
			return true
		}
		time.Sleep(time.Duration(sleep) * time.Millisecond)
	}
	return false
}