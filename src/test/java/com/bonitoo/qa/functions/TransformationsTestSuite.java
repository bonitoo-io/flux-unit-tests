package com.bonitoo.qa.functions;

/*
  Test transformations e.g.
     join
     union
     pivot
     limit
     etc.
 */

import com.bonitoo.qa.SetupTestSuite;
import org.influxdata.client.QueryApi;
import org.influxdata.query.FluxRecord;
import org.influxdata.query.FluxTable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TransformationsTestSuite {

    private static final Logger LOG = LoggerFactory.getLogger(TransformationsTestSuite.class);
    private static QueryApi queryClient = SetupTestSuite.getInfluxDBClient().getQueryApi();


    //Cumulative Sum
    @Test
    public void cumulativeSumTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -6h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"CO\")\n" +
                "  |> filter(fn: (r) => r.gps == \"50.03.41 14.24.32\")\n" +
                "  |> cumulativeSum(columns: [\"_value\"])",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set


        Integer[] sumVals = {12, 28, 47, 69, 91, 112, 130, 145, 159, 171};

        int sumValsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat((Double)rec.getValue()).isEqualTo(Double.valueOf(sumVals[sumValsCt++]));
        }

    }

    @Test
    public void columnsTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                "  |> filter(fn: (r) => r.gps == \"50.03.41 14.24.32\")\n" +
                "  |> columns(column: \"_value\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        String[] colVals = {"_start", "_stop", "_time", "_value", "_field",
                "_measurement", "city", "gps", "label", "location", "unitId"};

        int colValsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat((String)rec.getValue()).isEqualTo(colVals[colValsCt++]);
        }

    }

    //Drop
    @Test
    public void dropTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                        "  |> drop(columns: [\"gps\", \"label\"])",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isEqualTo(2); //one for each monitor tag set

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(SetupTestSuite.columnsContains(table.getColumns(), "gps")).as("Columns should not contain [gps]").isFalse();
            assertThat(SetupTestSuite.columnsContains(table.getColumns(), "label")).as("Columns should not contain [label]").isFalse();
            assertThat(SetupTestSuite.columnsContains(table.getColumns(), "_value")).as("Columns should contain [_value]").isTrue();
        });

    }

    //duplicate
    @Test
    public void duplicateTest(){

        String query = String.format("from(bucket: \"test-data\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"O3\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "  |> duplicate(column: \"_value\", as: \"instval\")\n" +
                "  |> cumulativeSum(columns: [\"_value\"])",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        Integer[] instVals = {157, 167, 172, 175, 176, 174, 172, 171, 169, 170 };

        long cumulative = 0;

        int instValsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){

            assertThat(rec.getValueByKey("instval")).isEqualTo(Long.valueOf(instVals[instValsCt++]));
            cumulative += (Long)rec.getValueByKey("instval");
            assertThat((Long)rec.getValue()).isEqualTo(cumulative);
        }

    }

    //Join
    @Test
    public void joinTest(){

        String query = String.format("Smichov = from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\") \n" +
                "  |> filter(fn: (r) => r._field == \"SO2\")\n" +
                "  |> drop(columns: [\"gps\", \"label\", \"location\", \"unitId\" ])\n" +
                "  \n" +
                "Hlavni = from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r.location == \"Praha hlavni\") \n" +
                "  |> filter(fn: (r) => r._field == \"SO2\")\n" +
                "  |> drop(columns: [\"gps\", \"label\", \"location\", \"unitId\" ])\n" +
                "  \n" +
                "  \n" +
                "join(tables: {key1: Smichov, key2: Hlavni}, on: [\"city\", \"_field\", \"_measurement\", \"_start\", \"_stop\", \"_time\" ], method: \"inner\")",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        Integer[] so21vals = {74, 79, 88, 86, 85, 84, 84, 82, 82, 81};
        Integer[] so22vals = {57, 54, 54, 58, 61, 60, 57, 62, 64, 63};

        int so2ValsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){

            assertThat(rec.getValueByKey("_value_key1")).isEqualTo(Long.valueOf(so21vals[so2ValsCt]));
            assertThat(rec.getValueByKey("_value_key2")).isEqualTo(Long.valueOf(so22vals[so2ValsCt++]));

        }


    }

}
