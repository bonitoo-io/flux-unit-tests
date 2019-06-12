package com.bonitoo.qa.dashboards;

import com.bonitoo.qa.SetupTestSuite;
import org.influxdata.client.QueryApi;
import org.influxdata.query.FluxTable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

/*
Queries taken manually from Cells in System-flux.json - 08.02.2019
https://gitlab.com/bonitoo-io/influxdata/blob/master/chronograf-protoboards/System-Flux.json

Current test is POC

TODO - find way to keep these queries synched with dashboards to be released
 */

public class SystemDashTestSuite {

    private static final Logger LOG = LoggerFactory.getLogger(SystemDashTestSuite.class);
    private static QueryApi queryClient = SetupTestSuite.getInfluxDBClient().getQueryApi();

    //Not in System-flux.json - however this is the standard minimum query
    @Test
    public void CpuUsageIdleTest(){
        String query = String.format("timeRangeStart = -15m\n\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: timeRangeStart)\n" +
                "  |> filter(fn: (r) => r._measurement == \"cpu\")\n" +
                "  |> filter(fn: (r) => r._field == \"usage_idle\")\n" +
                "  |> filter(fn: (r) => r.cpu == \"cpu-total\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        //System.out.println("DEBUG query: " + query);

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

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
        SetupTestSuite.printTables(query, tables);
    }

    @Test
    public void SystemNCpusTest(){

        int n_cpus = Runtime.getRuntime().availableProcessors();

        String query = String.format("dashboardTime = -15m\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"system\" and (r._field == \"n_cpus\"))",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

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
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getHostname());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

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
                "  |> map(fn: (r) => ({_time: r._time, _value: r._value / 1}))\n" +
                "  |> window(every: autoInterval)\n" +
                "  |> last()\n" +
                "  |> group(columns: [\"_time\", \"_start\", \"_stop\", \"_value\"], mode: \"except\")",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getHostname());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

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
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getHostname());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        SetupTestSuite.printTables(query, tables);

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

        String query = String.format("dashboardTime = -15m\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"diskio\" and r.host==\"%s\" and (r._field == \"read_bytes\" or r._field == \"write_bytes\"))\n" +
                "  |> derivative(unit: 1s, nonNegative: true, columns: [\"_value\"])\n" +
                "  |> group(columns: [\"_field\",\"name\"], mode: \"by\")",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getHostname());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isGreaterThan(0);
            table.getRecords().forEach(record -> {
                assertThat(record.getMeasurement()).isEqualTo("diskio");
                assertThat(record.getField().equals("read_bytes") ||
                        record.getField().equals("write_bytes")).isTrue();
                assertThat(record.getValue() instanceof Double).isTrue();
                assertThat((Double) record.getValue()).isGreaterThanOrEqualTo(0);
            });
        });

    }

    @Test
    public void ProcessesTest(){

        String query = String.format("dashboardTime = -15m\n" +
                "autoInterval = 30s\n\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"processes\" and r.host == \"%s\" and r._field == \"total\")\n" +
                "  |> window(every: autoInterval)\n" +
                "  |> group(columns: [\"_field\"], mode: \"by\")",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getHostname());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isGreaterThan(0);
            table.getRecords().forEach(record -> {
                assertThat(record.getMeasurement()).isEqualTo("processes");
                assertThat(record.getField()).isEqualTo("total");
                assertThat(record.getValue() instanceof Long).isTrue();
                assertThat((Long) record.getValue()).isGreaterThanOrEqualTo(0);
            });
        });

    }

    @Test
    public void SwapTest(){

        String query = String.format("dashboardTime = -15m\n" +
                "autoInterval = 30s\n\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"swap\" and r.host == \"%s\" and (r._field == \"used\" or r._field == \"total\"))\n" +
                "  |> window(every: autoInterval)\n" +
                "  |> group(columns: [\"_field\"], mode: \"by\")",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getHostname());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isGreaterThan(0);
            table.getRecords().forEach(record -> {
                assertThat(record.getMeasurement()).isEqualTo("swap");
                assertThat(record.getField().equals("total") ||
                        record.getField().equals("used")).isTrue();
                assertThat(record.getValue() instanceof Long).isTrue();
                assertThat((Long) record.getValue()).isGreaterThanOrEqualTo(0);
            });
        });

    }

    @Test
    public void MemoryUsage(){

        String query = String.format("dashboardTime = -15m\n" +
                "autoInterval = 30s\n\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"mem\" and r.host == \"%s\" and (r._field == \"used_percent\"))\n" +
                "  |> window(every: autoInterval)\n" +
                "  |> group(columns: [\"_field\"], mode: \"by\")\n",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getHostname());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isGreaterThan(0);
            table.getRecords().forEach(record -> {
                assertThat(record.getMeasurement()).isEqualTo("mem");
                assertThat(record.getField()).isEqualTo("used_percent");
                assertThat(record.getValue() instanceof Double).isTrue();
                assertThat((Double) record.getValue()).isGreaterThanOrEqualTo(0);
            });
        });

    }

    @Test
    public void FilesystemsUsage(){

        String query = String.format("dashboardTime = -15m\n" +
                "autoInterval = 30s\n\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"disk\" and (r._field == \"used_percent\") and r.host == \"%s\" )\n" +
                "  |> window(every: autoInterval)\n" +
                "  |> mean()\n" +
                "  |> group(columns: [\"_field\",\"path\"], mode: \"by\")",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getHostname());

        System.out.println("DEBUG query: " + query);

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isGreaterThan(0);
            table.getRecords().forEach(record -> {
                assertThat(record.getMeasurement()).isEqualTo("disk");
                assertThat(record.getField()).isEqualTo("used_percent");
                assertThat(record.getValue() instanceof Double).isTrue();
                assertThat((Double) record.getValue()).isGreaterThanOrEqualTo(0);
            });
        });

    }

    @Test
    public void SystemLoad1Test(){

        String query = String.format("dashboardTime = -15m\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"system\" and (r._field == \"load1\"))",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isGreaterThan(0);
            table.getRecords().forEach(record -> {
                assertThat(record.getMeasurement()).isEqualTo("system");
                assertThat(record.getField()).isEqualTo("load1");
                assertThat(record.getValue() instanceof Double).isTrue();
                assertThat((Double) record.getValue()).isGreaterThanOrEqualTo(0);
            });
        });

    }

    @Test
    public void TotalMemoryTest(){

        String query = String.format("dashboardTime = -15m\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"mem\" and (r._field == \"total\"))\n" +
                "  |> map(fn: (r) => r._value/1024/1024/1024)",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isGreaterThan(0);
            table.getRecords().forEach(record -> {
                assertThat(record.getMeasurement()).isEqualTo("mem");
                assertThat(record.getField()).isEqualTo("total");
                assertThat(record.getValue() instanceof Long).isTrue();
                assertThat((Long) record.getValue()).isGreaterThan(0);
            });
        });

    }

    @Test
    public void CpuUsage(){

        String query = String.format("dashboardTime = -15m\n" +
                "autoInterval = 30s\n\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: dashboardTime)\n" +
                "  |> filter(fn: (r) => r._measurement == \"cpu\"\n" +
                "  and \t(r._field == \"usage_system\" or r._field == \"usage_user\" or r._field == \"usage_idle\")\n  and r.host == \"%s\"\n" +
                "  and     r.cpu == \"cpu-total\" )\n" +
                "  |> window(every: autoInterval)\n" +
                "  |> group(columns: [\"_field\"], mode: \"by\")",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getHostname());

        System.out.println("DEBUG query: " + query);

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isGreaterThan(0);
            table.getRecords().forEach(record -> {
                assertThat(record.getMeasurement()).isEqualTo("cpu");
                assertThat(record.getField().equals("usage_idle") ||
                        record.getField().equals("usage_system") ||
                        record.getField().equals("usage_user")).isTrue();
                assertThat(record.getValue() instanceof Double).isTrue();
                assertThat((Double) record.getValue() >= 0.0 &&
                        (Double) record.getValue() < 100.0).isTrue();
            });
        });


    }

}
