## First pass

First pass in an afternoon of putting together a "unit" test framework for flux queries.

##Restart Influx2

First restart the target server from the commandline.

TODO - make start and start server part of suite.

See the testConfig.yml for the target endpoint.

##Run tests

```
mvn --fail-at-end -Dmaven.test.failure.ignore=true clean test surefire-report:report
```

TODO - find a more elegant way to configure test run.

## Report

HTML report is in...

```
target/site/surefire-report.html
```



