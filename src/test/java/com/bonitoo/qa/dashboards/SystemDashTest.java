package com.bonitoo.qa.dashboards;

import com.bonitoo.qa.TestRunner;
import org.influxdata.client.flux.domain.FluxTable;
import org.influxdata.java.client.QueryApi;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

public class SystemDashTest {

    private static final Logger LOG = LoggerFactory.getLogger(SystemDashTest.class);
    private static QueryApi queryClient = TestRunner.getInfluxDBClient().getQueryApi();

    @Test
    public void CPUTest(){
        String query = String.format("timeRangeStart = -1h\n\n" +
                "from(bucket: \"%s\")\n" +
                "  |> range(start: timeRangeStart)\n" +
                "  |> filter(fn: (r) => r._measurement == \"cpu\")\n" +
                "  |> filter(fn: (r) => r._field == \"usage_idle\")\n" +
                "  |> filter(fn: (r) => r.cpu == \"cpu-total\")",
                TestRunner.getTestConf().getOrg().getBucket());

        System.out.println("DEBUG query: " + query);

        List<FluxTable> tables = queryClient.query(query, TestRunner.getInflux2conf().getOrgId());

        assertThat(tables.size()).isGreaterThan(0);

        TestRunner.printTables(query, tables);
    }


}
