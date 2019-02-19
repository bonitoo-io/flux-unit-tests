package com.bonitoo.qa.functions;

import com.bonitoo.qa.SetupTestSuite;
import org.influxdata.client.QueryApi;
import org.influxdata.query.FluxTable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SelectorsTestSuite {

    private static final Logger LOG = LoggerFactory.getLogger(SelectorsTestSuite.class);
    private static QueryApi queryClient = SetupTestSuite.getInfluxDBClient().getQueryApi();


    @BeforeClass
    public static void setup(){

        SetupTestSuite.setupAirRecords();

    }

    @Test
    public void bottomTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"pressure\")\n" +
                        "  |> bottom(n:3)",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        tables.forEach(table ->{
            table.getRecords().forEach(rec -> {
                assertThat(rec.getValue()).isEqualTo(1019.0);
            });
        });

    }

    @Test
    public void distinctTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"battery-v\")\n" +
                        "  |> distinct(column: \"location\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat((String)tables.get(0).getRecords().get(0).getValue()).isEqualTo("Smichov");
        assertThat((String)tables.get(1).getRecords().get(0).getValue()).isEqualTo("Praha hlavni");

    }

    @Test
    public void firstTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"battery-v\")\n" +
                        "  |> first()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(2.1);
        assertThat((Double)tables.get(1).getRecords().get(0).getValue()).isEqualTo(3.0);
    }


    @Test
    public void lastTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                        "  |> last()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(37);
        assertThat((Double)tables.get(1).getRecords().get(0).getValue()).isEqualTo(57);
    }


}
