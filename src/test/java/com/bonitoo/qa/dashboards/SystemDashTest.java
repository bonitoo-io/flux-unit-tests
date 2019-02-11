package com.bonitoo.qa.dashboards;

import com.bonitoo.qa.CLIWrapper;
import com.bonitoo.qa.TestRunner;
import org.influxdata.client.QueryApi;
import org.influxdata.query.FluxTable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

/*
Queries taken manually from Cells in System-flux.json - 08.02.2019
https://gitlab.com/bonitoo-io/influxdata/blob/master/chronograf-protoboards/System-Flux.json

Current test is POC

TODO - find way to keep these queries synched with dashboards to be released
 */

public class SystemDashTest {

    private static final Logger LOG = LoggerFactory.getLogger(SystemDashTest.class);
    private static QueryApi queryClient = TestRunner.getInfluxDBClient().getQueryApi();

    //Not in System-flux.json - however this is the standard minimum query
    @Test
    public void CpuUsageIdleTest(){
        String query = String.format("timeRangeStart = -15m\n\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: timeRangeStart)\n" +
                "  |> filter(fn: (r) => r._measurement == \"cpu\")\n" +
                "  |> filter(fn: (r) => r._field == \"usage_idle\")\n" +
                "  |> filter(fn: (r) => r.cpu == \"cpu-total\")",
                TestRunner.getTestConf().getOrg().getBucket());

        //System.out.println("DEBUG query: " + query);

        List<FluxTable> tables = queryClient.query(query, TestRunner.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isGreaterThan(0);
            table.getRecords().forEach(record -> {
                assertThat(record.getMeasurement()).isEqualTo("cpu");
                assertThat(record.getField()).isEqualTo("usage_idle");
                assertThat(record.getValue() instanceof Double).isTrue();
                assertThat((Double)record.getValue() >= 0 && (Double)record.getValue() <= 100).isTrue();
            });
        });

        //for fun and inspection
        TestRunner.printTables(query, tables);
    }

    @Test
    public void SystemNCpusTest(){

        int n_cpus = Runtime.getRuntime().availableProcessors();

        String query = String.format("dashboardTime = -15m\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"system\" and (r._field == \"n_cpus\"))",
                TestRunner.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, TestRunner.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        //for fun and inspection
        TestRunner.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isGreaterThan(0);
            table.getRecords().forEach(record -> {
                assertThat(record.getMeasurement()).isEqualTo("system");
                assertThat(record.getField()).isEqualTo("n_cpus");
                assertThat(record.getValue() instanceof Long).isTrue();
                assertThat((Long)record.getValue()).isEqualTo(n_cpus);
            });

        });

    }
//                System.out.println("Debug value.getClass(): " + record.getValue().getClass());

    @Test
    public void SystemLoadTest(){

        String query = String.format("dashboardTime = -15m\n" +
                "autoInterval = 30s\n\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"system\" and r.host == \"%s\" and (r._field == \"load1\" or r._field == \"load15\" or r._field == \"load5\"))\n" +
                "  |> window(every: autoInterval)\n" +
                "  |> group(columns: [\"_field\"], mode: \"by\")",
                TestRunner.getTestConf().getOrg().getBucket(),
                TestRunner.getTestConf().getHostname());

        List<FluxTable> tables = queryClient.query(query, TestRunner.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        //for fun and inspection
        TestRunner.printTables(query, tables);

        tables.forEach(table -> {
                    assertThat(table.getRecords().size()).isGreaterThan(0);
                    table.getRecords().forEach(record -> {
                        assertThat(record.getMeasurement()).isEqualTo("system");
                        assertThat(record.getField().equals("load1") ||
                                record.getField().equals("load15") ||
                                record.getField().equals("load5")).isTrue();
                        assertThat(record.getValue() instanceof Double).isTrue();
                        assertThat((Double) record.getValue() >= 0 && (Double) record.getValue() <= 1);
                    });
                });

    }

    @Test
    public void SystemUptimeTest(){

        //changed r._value / 86400 to r._value / 1 because telegraf will have been up only a few minutes

        String query = String.format("dashboardTime = -15m\n" +
                "autoInterval = 30s\n\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"system\" and r.host == \"%s\" and (r._field == \"uptime\"))\n" +
                "  |> map(fn: (r) => ({_time: r._time, _value: r._value / 1))\n" +
                "  |> window(every: autoInterval)\n" +
                "  |> last()\n" +
                "  |> group(columns: [\"_time\", \"_start\", \"_stop\", \"_value\"], mode: \"except\")",
                TestRunner.getTestConf().getOrg().getBucket(),
                TestRunner.getTestConf().getHostname());

        List<FluxTable> tables = queryClient.query(query, TestRunner.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        //for fun and inspection
        TestRunner.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isGreaterThan(0);
            table.getRecords().forEach(record -> {
                assertThat(record.getMeasurement()).isEqualTo("system");
                assertThat(record.getField()).isEqualTo("uptime");
                assertThat(record.getValue() instanceof Long).isTrue();
                assertThat((Long) record.getValue()).isGreaterThanOrEqualTo(0);
            });
        });



    }

    @Test
    public void NetworkTest(){

        String query = String.format("dashboardTime = -15m\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"net\" and r.host==\"%s\" and (r._field == \"bytes_sent\" or r._field == \"bytes_recv\"))\n" +
                "  |> derivative(unit: 1s, nonNegative: true, columns: [\"_value\"])\n" +
                "  |> group(columns: [\"_field\"], mode: \"by\" )",
                TestRunner.getTestConf().getOrg().getBucket(),
                TestRunner.getTestConf().getHostname());

        System.out.println("DEBUG query: " + query);

        List<FluxTable> tables = queryClient.query(query, TestRunner.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        TestRunner.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isGreaterThan(0);
            table.getRecords().forEach(record -> {
                assertThat(record.getMeasurement()).isEqualTo("net");
                assertThat(record.getField().equals("bytes_recv") ||
                        record.getField().equals("bytes_sent")).isTrue();
                assertThat(record.getValue() instanceof Double).isTrue();
                assertThat((Double) record.getValue()).isGreaterThanOrEqualTo(0);
            });
        });


    }

    @Test
    public void DiskIOTest(){

    }

}
