# OOI Status HTTP API

## Data Availability

```
/available/<refdes> [GET]
```

Arguments:
* refdes (path argument) - The reference designator from which to query availability
* method (query argument) - Delivery method (accepts partial strings)
* stream (query argument) - Stream name (accepts partial strings)
* start_time (query argument) - Start time for the availability window
* stop_time (query argument) - Stop time for the availability window

Example query:

```
http://uframe-4-test:9000/available/RS03CCAL-MJ03F-05-BOTPTA301?stream=nano
```

Response:

```json
{
	"availability": [
		{
			"categories": {
				"Deployment: 1": {
					"color": "#0073cf"
				}
			},
			"data": [
				[
					"Fri, 25 Jul 2014 21:05:00 GMT",
					"Deployment: 1",
					"Mon, 30 Jan 2017 23:26:15 GMT"
				]
			],
			"measure": "Deployments"
		}
	]
}
```


## Data Status
### Expected

```
/expected [GET]
/expected/<int:expected_id> [GET, PATCH]
```

Arguments:
* method (query argument) - Delivery method (accepts partial strings) (/expected only)
* stream (query argument) - Stream name (accepts partial strings) (/expected only)
* expected_id (path argument) (/expected/<id> only)

This endpoint allows the user to read or set the values in an ExpectedStream object. This object defines the
base values for a particular stream upon which specific instances of this stream will be based. For example:

Query:

```
http://uframe-4-test:9000/expected/33
```

Response:

```json
{
	"expected_rate": 0,
	"fail_interval": 800,
	"id": 33,
	"method": "streamed",
	"name": "horizontal_electric_field",
	"warn_interval": 400
}
```

This object indicates the warning and failure intervals between subsequent data points (or bursts of data, depending
on the sample strategy for the instrument in question). When a DeployedStream (see below) exceeds these intervals
the backend status monitor will generate a status event to change the status to the corresponding state.

Only the fail_interval, warn_interval and expected_rate fields may be updated with a PATCH call. The id field is
required for a PATCH call and must match the corresponding expected_id in the URL.

### Deployed

```
/deployed/<int:deployed_id> [GET, PATCH]
```

Arguments:
* deployed_id (path argument) (/deployed/<id> only)

This endpoint allows the user to read or set the values in an DeployedStream object. This object defines the
specific values for a particular stream. For example:


Query:

```
http://uframe-4-test:9000/deployed/10
```

Response:

```json
{
	"expected_rate": null,
	"expected_stream": {
		"expected_rate": 0,
		"fail_interval": 600,
		"id": 23,
		"method": "streamed",
		"name": "ctdpf_optode_sample",
		"warn_interval": 120
	},
	"fail_interval": null,
	"id": 10,
	"reference_designator": "RS01SLBS-LJ01A-12-CTDPFB101",
	"reference_designator_id": 3,
	"warn_interval": null
}
```

Note that the expected stream corresponding to this deployed stream is included in the response. A value of null
for any of the interval/rate fields in this object indicates that this instance should use the default values supplied
in the expected stream object. Any non-null value will override the value from the expected stream and apply it to
this instance. Setting all three values (expected_rate, fail_interval and warn_interval) to zero overrides the normal
behavior and indicates that this stream should not be tracked.

Only the fail_interval, warn_interval and expected_rate fields may be updated with a PATCH call. The id field is
required for a PATCH call and must match the corresponding expected_id in the URL.

### Stream

```
/stream [GET]
```

Arguments:
* refdes (query argument) - The reference designator from which to query status (accepts partial strings)
* method (query argument) - Delivery method (accepts partial strings)
* stream (query argument) - Stream name (accepts partial strings)
* status (query argument) - Only return streams which match this status

```
/stream/<int:deployed_id> [GET]
```

Arguments:
* deployed_id (path argument) - Return status for a specific stream


#### Example

Query:

```
http://uframe-4-test:9000/stream?refdes=CTD&stream=ctdbp
```

Response:

```json
{
	"status": [
		{
			"last_status": "operational",
			"last_status_time": "Tue, 31 Jan 2017 00:14:00 GMT",
			"stream": {
				"expected_rate": null,
				"expected_stream": {
					"expected_rate": 0,
					"fail_interval": 600,
					"id": 15,
					"method": "streamed",
					"name": "ctdbp_no_sample",
					"warn_interval": 120
				},
				"fail_interval": null,
				"id": 15,
				"reference_designator": "CE04OSBP-LJ01C-06-CTDBPO108",
				"reference_designator_id": 7,
				"warn_interval": null
			}
		},
		{
			"last_status": "operational",
			"last_status_time": "Tue, 31 Jan 2017 00:14:00 GMT",
			"stream": {
				"expected_rate": null,
				"expected_stream": {
					"expected_rate": 0,
					"fail_interval": 600,
					"id": 15,
					"method": "streamed",
					"name": "ctdbp_no_sample",
					"warn_interval": 120
				},
				"fail_interval": null,
				"id": 24,
				"reference_designator": "CE02SHBP-LJ01D-06-CTDBPN106",
				"reference_designator_id": 12,
				"warn_interval": null
			}
		}
	]
}
```


```
/stream/<int:deployed_id>/disable [PUT]
/stream/<int:deployed_id>/enable [PUT]
```

These endpoints allow the user to enable/disable monitoring for a stream. Same as patching the specified object
with all zeros.

### Instrument

```
/instrument
```

This endpoint allows the user to query status by instrument (one or more streams). The overall status of the
instrument is equal to the worst status of all streams produced by that instrument.

Arguments:
* refdes (query argument) - The reference designator from which to query status (accepts partial strings)
* method (query argument) - Delivery method (accepts partial strings)
* stream (query argument) - Stream name (accepts partial strings)
* status (query argument) - Only return instruments which match this status

Query:

```
http://uframe-4-test:9000/instrument?status=failed&refdes=301
```

Response:

```json
{
	"RS03AXPS-PC03A-06-VADCPA301": {
		"overall": "failed",
		"status": [
			{
				"last_status": "failed",
				"last_status_time": "Tue, 31 Jan 2017 00:26:00 GMT",
				"stream": {
					"expected_rate": null,
					"expected_stream": {
						"expected_rate": 0,
						"fail_interval": 1120,
						"id": 78,
						"method": "streamed",
						"name": "adcp_config",
						"warn_interval": 224
					},
					"fail_interval": null,
					"id": 113,
					"reference_designator": "RS03AXPS-PC03A-06-VADCPA301",
					"reference_designator_id": 34,
					"warn_interval": null
				}
			}
		]
	},
	"RS03AXPS-SF03A-4A-NUTNRA301": {
		"overall": "failed",
		"status": [
			{
				"last_status": "failed",
				"last_status_time": "Tue, 31 Jan 2017 00:26:00 GMT",
				"stream": {
					"expected_rate": null,
					"expected_stream": {
						"expected_rate": 0,
						"fail_interval": 5536,
						"id": 121,
						"method": "streamed",
						"name": "nutnr_a_dark_sample",
						"warn_interval": 1107
					},
					"fail_interval": null,
					"id": 105,
					"reference_designator": "RS03AXPS-SF03A-4A-NUTNRA301",
					"reference_designator_id": 59,
					"warn_interval": null
				}
			}
		]
	}
}
```

```
/instrument/<int:refdes_id>
```

Query:

```
http://uframe-4-test:9000/instrument/2
```

Response:

```json
{
	"overall": "failed",
	"status": [
		{
			"last_status": "operational",
			"last_status_time": "Tue, 31 Jan 2017 00:23:00 GMT",
			"stream": {
				"expected_rate": null,
				"expected_stream": {
					"expected_rate": 0,
					"fail_interval": 600,
					"id": 44,
					"method": "streamed",
					"name": "adcp_engineering",
					"warn_interval": 120
				},
				"fail_interval": null,
				"id": 9,
				"reference_designator": "RS01SLBS-LJ01A-10-ADCPTE101",
				"reference_designator_id": 2,
				"warn_interval": null
			}
		},
		{
			"last_status": "operational",
			"last_status_time": "Tue, 31 Jan 2017 00:23:00 GMT",
			"stream": {
				"expected_rate": null,
				"expected_stream": {
					"expected_rate": 0,
					"fail_interval": 600,
					"id": 26,
					"method": "streamed",
					"name": "adcp_velocity_beam",
					"warn_interval": 120
				},
				"fail_interval": null,
				"id": 8,
				"reference_designator": "RS01SLBS-LJ01A-10-ADCPTE101",
				"reference_designator_id": 2,
				"warn_interval": null
			}
		},
		{
			"last_status": "failed",
			"last_status_time": "Tue, 31 Jan 2017 00:23:00 GMT",
			"stream": {
				"expected_rate": null,
				"expected_stream": {
					"expected_rate": 0,
					"fail_interval": 1120,
					"id": 78,
					"method": "streamed",
					"name": "adcp_config",
					"warn_interval": 224
				},
				"fail_interval": null,
				"id": 115,
				"reference_designator": "RS01SLBS-LJ01A-10-ADCPTE101",
				"reference_designator_id": 2,
				"warn_interval": null
			}
		}
	]
}
```


```
/instrument/<int:refdes_id>/disable [PUT]
/instrument/<int:refdes_id>/enable [PUT]
```

This endpoint allows the user to enable / disable monitoring for an entire instrument in a single call.
