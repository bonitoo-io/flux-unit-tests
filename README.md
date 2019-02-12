## First pass

First pass in an afternoon of putting together a "unit" test framework for flux queries.

## Restart Influx2

First restart the target server from the commandline.

TODO - make start and start server part of suite.

See the testConfig.yml for the target endpoint.

## Run tests

```
$ mvn --fail-at-end -Dmaven.test.failure.ignore=true clean test surefire-report:report
```

TODO - find a more elegant way to configure test run.

Console results.

```
...
[INFO] -------------------------------------------------------
[INFO]  T E S T S
[INFO] -------------------------------------------------------
[INFO] Running com.bonitoo.qa.SetupTestSuite
DEBUG setting up lowest level class in file system
com.bonitoo.qa.config.Org@7ba18f1b[
  name=qa
  admin=admin
  password=changeit
  bucket=test-data
]
com.bonitoo.qa.config.Influx2@2f8f5f62[
  url=http://localhost:9999
  api=/api/v2
]
Something test
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.679 s - in com.bonitoo.qa.SetupTestSuite
[INFO] Running com.bonitoo.qa.flux.rest.artifacts.test.ArtifactsTestSuite
Feb 01, 2019 4:48:55 PM org.influxdata.platform.impl.AbstractWriteClient close
INFO: Flushing any cached BatchWrites before shutdown.
OrgId: 03566f41d3d44000
First Bucket: 03566f41d4d44000
Token: daxn6yu4ecRlylLFLQixJj6BdmmKnCsMw5cXfrjHJFDW_SepsOoMV7Hq1uZZFODLUsee7bZmWFmaoVkvCj7zeg==
16:48:55.597 [main] INFO  c.b.q.f.r.a.test.ArtifactsTest -

Query:

from(bucket: "test-data")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "weather_outdoor")
  |> filter(fn: (r) => r.home == "100")
  |> filter(fn: (r) => r.sensor == "120")
  |> last()

Result:

┌─────────────────────┬─────────────────────┬────────────────────┬────────────────────┬────────────────────┬────────────────────┬────────────────────┐
│table                │_start               │_stop               │_time               │_measurement        │_field              │_value              │
├─────────────────────┼─────────────────────┼────────────────────┼────────────────────┼────────────────────┼────────────────────┼────────────────────┤
│0                    │1970-01-01 01:00:00  │2019-02-01 16:48:55 │2019-01-30 13:28:56 │weather_outdoor     │battery_voltage     │2.6                 │
├─────────────────────┼─────────────────────┼────────────────────┼────────────────────┼────────────────────┼────────────────────┼────────────────────┤
│1                    │1970-01-01 01:00:00  │2019-02-01 16:48:55 │2019-01-30 13:28:56 │weather_outdoor     │precipitation       │865                 │
├─────────────────────┼─────────────────────┼────────────────────┼────────────────────┼────────────────────┼────────────────────┼────────────────────┤
│2                    │1970-01-01 01:00:00  │2019-02-01 16:48:55 │2019-01-30 13:28:56 │weather_outdoor     │pressure            │880                 │
├─────────────────────┼─────────────────────┼────────────────────┼────────────────────┼────────────────────┼────────────────────┼────────────────────┤
│3                    │1970-01-01 01:00:00  │2019-02-01 16:48:55 │2019-01-30 13:28:56 │weather_outdoor     │wind_speed          │11                  │
└─────────────────────┴─────────────────────┴────────────────────┴────────────────────┴────────────────────┴────────────────────┴────────────────────┘

Orgname: qa
Admin: admin
Password: changeit
Bucket: test-data
Influx2 URL: http://localhost:9999
Influx2 API: /api/v2
[ERROR] Tests run: 7, Failures: 1, Errors: 0, Skipped: 2, Time elapsed: 0.244 s <<< FAILURE! - in com.bonitoo.qa.flux.rest.artifacts.test.ArtifactsTestSuite
[ERROR] testFail(com.bonitoo.qa.flux.rest.artifacts.test.ArtifactsTestSuite)  Time elapsed: 0.007 s  <<< FAILURE!
org.junit.ComparisonFailure: expected:<[fals]e> but was:<[tru]e>
	at com.bonitoo.qa.flux.rest.artifacts.test.ArtifactsTestSuite.testFail(ArtifactsTest.java:147)

[INFO]
[INFO] Results:
[INFO]
[ERROR] Failures:
[ERROR]   ArtifactsTest.testFail:147 expected:<[fals]e> but was:<[tru]e>
[INFO]
[ERROR] Tests run: 8, Failures: 1, Errors: 0, Skipped: 2
[INFO]
...
```

## Report

HTML report is in...

```
target/site/surefire-report.html
```



