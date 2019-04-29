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

        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, "Smichov")).isTrue();
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, "Praha hlavni")).isTrue();

        /*
        assertThat((String)tables.get(0).getRecords().get(0).getValue()).isEqualTo("Smichov");
        assertThat((String)tables.get(1).getRecords().get(0).getValue()).isEqualTo("Praha hlavni");
        */

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

        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 2.1)).isTrue();
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 3.0)).isTrue();

        /*
        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(2.1);
        assertThat((Double)tables.get(1).getRecords().get(0).getValue()).isEqualTo(3.0);
        */
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

        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 37.0)).isTrue();
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 57.0)).isTrue();
/*
        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(37);
        assertThat((Double)tables.get(1).getRecords().get(0).getValue()).isEqualTo(57);
        */
    }

    @Test
    public void maxTest(){
        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"CO\")\n" +
                "  |> max()",
                SetupTestSuite.getTestConf().getOrg().getBucket());
        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 22.0));
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 8.3));

        /*
        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(22.0);
        assertThat((Double)tables.get(1).getRecords().get(0).getValue()).isEqualTo(8.3);
        */
    }

    @Test
    public void minTest(){
        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"CO\")\n" +
                        "  |> min()",
                SetupTestSuite.getTestConf().getOrg().getBucket());
        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 12.0)).isTrue();
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 3.8)).isTrue();

        /*
        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(12.0);
        assertThat((Double)tables.get(1).getRecords().get(0).getValue()).isEqualTo(3.8);
        */

    }

    @Test
    public void sampleTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "  |> sample(n: 4, pos: 2)",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        Double[] vals = {40.0, 48.0, 40.0, 37.0, 36.0};

        int valsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat((Double)rec.getValue()).isEqualTo(vals[valsCt++]);
        }

    }

    @Test
    public void topTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm10\")\n" +
                "  |> top(n: 3, columns: [\"_value\"])\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        Double[][] vals = { {217.0, 217.0, 217.0}, {145.0, 144.0, 143.0}};

        assertThat(FluxUtils.tablesContainsRecordsWithValsInOrder(tables,vals[0])).isTrue();
        assertThat(FluxUtils.tablesContainsRecordsWithValsInOrder(tables,vals[1])).isTrue();

        /*
        int valsCt = 0;

        for(FluxTable table : tables){
            for(FluxRecord rec : table.getRecords()){
                assertThat((Double)rec.getValue()).isEqualTo(vals[valsCt++]);
            }
        }
        */

    }

    @Test
    public void uniqueTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"battery-v\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "  |> unique(column: \"_value\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        Double[] vals = {2.1, 2.0};

        int valsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat((Double)rec.getValue()).isEqualTo(vals[valsCt++]);
        }

    }

    @Test
    public void highestAvgTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm10\")\n" +
                "  |> highestAverage(n: 5, column: \"_value\")\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(160.95);

    }

    @Test
    public void highestMaxTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm10\")\n" +
                "  |> window(every: 30m)\n" +
                "  |> highestMax(n: 3, groupColumns: [\"unitId\"])",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(217.0);
        assertThat((Double)tables.get(0).getRecords().get(1).getValue()).isEqualTo(145.0);

    }

    @Test
    public void lowestMinTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm10\")\n" +
                "  |> window(every: 30m)\n" +
                "  |> lowestMin(n: 10, groupColumns: [\"unitId\"])\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(99.0);
        assertThat((Double)tables.get(0).getRecords().get(1).getValue()).isEqualTo(170.0);
    }

    @Test
    public void lowestAvgTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm10\")\n" +
                "  |> window(every: 30m)\n" +
                "  |> lowestAverage(n: 1, groupColumns: [\"unitId\"])\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(120.6);
    }

    @Test
    public void highestCurrentTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"ppm10\")\n" +
                        "  |> window(every: 30m)\n" +
                        "  |> highestCurrent(n: 1, groupColumns: [\"unitId\"])\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(198.0);

    }

    @Test
    public void lowestCurrentTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"ppm10\")\n" +
                        "  |> window(every: 30m)\n" +
                        "  |> lowestCurrent(n: 1, groupColumns: [\"unitId\"])\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(139.0);

    }




}
