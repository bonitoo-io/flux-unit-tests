package com.bonitoo.qa.functions;

import com.bonitoo.qa.SetupTestSuite;
import com.bonitoo.qa.influx2.FluxUtils;
import org.influxdata.client.QueryApi;
import org.influxdata.query.FluxRecord;
import org.influxdata.query.FluxTable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class OutputsTestSuite {

    private static final Logger LOG = LoggerFactory.getLogger(OutputsTestSuite.class);
    private static QueryApi queryClient = SetupTestSuite.getInfluxDBClient().getQueryApi();


    @BeforeClass
    public static void setup(){

        SetupTestSuite.setupAirRecords();

    }


    /*
    from(bucket: "test-data")
  |> range(start: -4h, stop: now())
  |> filter(fn: (r) => r._measurement == "air_quality")
  |> filter(fn: (r) => r._field == "humidity")
  |> filter(fn: (r) => r.city == "Praha")
  |> map(fn: (r) => ({_time: r._time, _measurement: r._measurement, _field: r._field, _value: 100.0 - r._value; location: r.location}), mergeKey: false)
  |> timeShift(duration: -4h, columns: ["_time"])
  |> set(key: "_field", value: "dryness")
  |> to(bucket: "test-data", org: "qa")
     */

    @Test
    public void toInfluxdbTest(){

        /*String queryOut = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"humidity\")\n" +
                "  |> filter(fn: (r) => r.city == \"Praha\")\n" +
                "  |> map(fn: (r) => ({_time: r._time, _measurement: r._measurement, _field: r._field, _value: 100.0 - r._value}), mergeKey: false)\n" +
                "  |> timeShift(duration: -4h, columns: [\"_time\"])\n" +
                "  |> set(key: \"_field\", value: \"dryness\")\n" +
                "  |> to(bucket: \"%s\", org: \"%s\")",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getOrg().getName()); */

        String queryOut = String.format( "    from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"humidity\")\n" +
                "  |> filter(fn: (r) => r.city == \"Praha\")\n" +
                "  |> map(fn: (r) => ({_time: r._time, _measurement: r._measurement, _field: r._field, _value: 100.0 - r._value, location: r.location}), mergeKey: false)\n" +
                "  |> timeShift(duration: -4h, columns: [\"_time\"])\n" +
                "  |> set(key: \"_field\", value: \"dryness\")\n" +
                "  |> to(bucket: \"%s\", org: \"%s\")\n",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getOrg().getName());

        String queryIn = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -12h, stop: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"dryness\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        System.out.println("DEBUG queryOut: " + queryOut);
        System.out.println("DEBUG queryIn: " + queryIn);

        List<FluxTable> tablesOut = queryClient.query(queryOut, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(queryOut, tablesOut);


        List<FluxTable> tablesIn = queryClient.query(queryIn, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(queryOut, tablesOut);

        SetupTestSuite.printTables(queryIn, tablesIn);

        assertThat(tablesIn.size()).isEqualTo(2);

        Double[] vals = {32.0, 31.5, 32.0, 32.5, 32.5, 33.0, 33.5, 34.0, 34.0, 34.5,
                         34.5, 34.0, 34.0, 34.0, 33.5, 33.0, 33.0, 31.5, 32.0, 32.0 };



        int valsCt = 0;

        assertThat(FluxUtils.tablesContainsRecordsWithValsInOrder(tablesIn, vals)).isTrue();


    }
}
