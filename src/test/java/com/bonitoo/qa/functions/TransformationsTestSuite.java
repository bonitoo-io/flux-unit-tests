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
import com.bonitoo.qa.influx2.FluxUtils;
import org.assertj.core.util.VisibleForTesting;
import org.influxdata.client.QueryApi;
import org.influxdata.client.WriteApi;
import org.influxdata.client.write.Point;
import org.influxdata.query.FluxRecord;
import org.influxdata.query.FluxTable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TransformationsTestSuite {

    private static final Logger LOG = LoggerFactory.getLogger(TransformationsTestSuite.class);
    private static QueryApi queryClient = SetupTestSuite.getInfluxDBClient().getQueryApi();


    @BeforeClass
    public static void setup(){

        SetupTestSuite.setupAirRecords();

        //try and setup additional data set for fill

        /*
        long recordInterval = 2 * 60000;
        long time = System.currentTimeMillis() - ((101) * recordInterval);

        WriteApi writeClient = SetupTestSuite.getInfluxDBClient().getWriteApi();


        for(int i = 100; i >= 0; i--){
            Point p = Point.measurement("foo")
            .addTag("rabbit", "peter");

            if(i % 5 == 0){
                p.addField("x", Long.valueOf(i));
            }else{
                p.addField("x",  Long.valueOf(-1));
            }

            p.time(time += recordInterval, WritePrecision.MS);

            writeClient.writePoint(SetupTestSuite.getInflux2conf().getBucketIds().get(0),
                    SetupTestSuite.getInflux2conf().getOrgId(),
                    p);

        }

        writeClient.close();
*/
    }


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

        List<String> colValsList = new ArrayList<String>();

        for(String s : colVals){
            colValsList.add(s);
        }

        List<String> recValsList = new ArrayList<String>();

        //assert That all records are in colVals since order is not determined
        for(FluxRecord rec : tables.get(0).getRecords()){
            recValsList.add((String)rec.getValue());
                assertThat(colValsList.contains(rec.getValue()))
                        .as(String.format("record %s is in column Value list", rec.getValue()))
                        .isTrue();
        }

        //assert That all column vals are in records table since order is not determined
        for(String s : colValsList){
            assertThat(recValsList.contains(s))
                    .as(String.format("column %s is in record list", s))
                    .isTrue();
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

    @Test
    public void keepTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"CO\")\n" +
                "  |> filter(fn: (r) => r.city == \"Praha\")\n" +
                "  |> keep(columns: [\"_time\", \"_value\", \"_field\", \"_measurement\", \"location\"])\n" +
                "  |> columns(column: \"col_labels\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2); //one for each monitor tag set

        String[] colLabels = {"_time", "_value", "_field", "_measurement", "location"};

        List<String> colLabelsList = new ArrayList<String>();

        for(String s : colLabels){
            colLabelsList.add(s);
        }

        List<String> recValsList = new ArrayList<String>();


        tables.forEach(table -> {
            //since order is not always determined
            for(FluxRecord rec : table.getRecords()){
                recValsList.add((String)rec.getValueByKey("col_labels"));
                assertThat(colLabelsList.contains(rec.getValueByKey("col_labels")))
                        .as(String.format("rec %s should be among column labels", rec.getValueByKey("col_labels")))
                        .isTrue();
            }
        });

        //since order is not always determined check that labels are among results
        for(String s : colLabelsList){
            assertThat(recValsList.contains(s))
                    .as("label %s should be among records", s)
                    .isTrue();
        }

    }

    @Test
    public void keysTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"SO2\")\n" +
                "  |> filter(fn: (r) => r.city == \"Praha\")\n" +
                "  |> keep(columns: [\"_value\", \"_time\", \"_field\", \"_measurement\", \"location\"])\n" +
                "  |> keys(column: \"_value\")\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2); //one for each monitor tag set

        String[] keyLabels = {"_field", "_measurement", "location"};

        List<String> keyLabelsList = new ArrayList<String>();

        for(String s : keyLabels){
            keyLabelsList.add(s);
        }

        List<String> recValsList = new ArrayList<String>();

        tables.forEach(table -> {
            for(FluxRecord rec : table.getRecords()){
                recValsList.add((String)rec.getValue());
                assertThat(keyLabelsList.contains(rec.getValue()))
                        .as(String.format("rec %s should be among keys", rec.getValue()))
                        .isTrue();
            }
        });

        //since order is not always determined check that labels are among results
        for(String s : keyLabelsList){
            assertThat(recValsList.contains(s))
                    .as("key %s should be among records", s)
                    .isTrue();
        }
    }

    @Test
    public void keyValsTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field != \"dryness\")\n" +
                        "  |> drop(columns: [\"_start\", \"_stop\"])\n" +
                        "  |> keep(columns: [\"_time\", \"_value\", \"_field\", \"_measurement\"])\n" +
                        "  |> keyValues(keyColumns: [\"_field\"] )",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(10); //one for each monitor tag set
        // Can be run after OutputsTestSuite which will raise the count
        // assertThat(tables.size() == 10 || tables.size() == 11).isTrue();


        // run before OutputsTestSuite
            String[] keyVals = {"CO", "NO2", "O3", "SO2", "battery-v", "humidity", "ppm025", "ppm10", "pressure", "temp"};

            int keyValsCt = 0;

            for(FluxTable table : tables){

                assertThat(table.getRecords().get(0).getValue()).isEqualTo(keyVals[keyValsCt++]);

            }

    }

    @Test
    public void limitTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"humidity\")\n" +
                "  |> limit(n:5, offset: 2)",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2); //one for each monitor tag set

        Double[][] vals = {{73.5, 73.0, 72.8, 72.6, 72.8}, {68.0, 67.5, 67.5, 67.0, 66.5}};

        assertThat(FluxUtils.tablesContainsRecordsWithValsInOrder(tables, vals[0]));

        /*
        int valsCt = 0;

        for(FluxTable table : tables){
            assertThat(table.getRecords().size()).isEqualTo(5);
            for(FluxRecord rec : table.getRecords()){
                assertThat(rec.getValue()).isEqualTo(vals[valsCt++]);
            }
        }*/


    }


    @Test
    public void mapTest(){

        String query = String.format("lowest = 100.0\n" +
                        "\n" +
                        "from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"ppm10\")\n" +
                        "  |> filter(fn: (r) => r.location == \"Praha hlavni\")\n" +
                        "  |> map(fn: (r) => r._value - lowest, mergeKey: true)",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        Double[] vals = {10.0, 7.0, 6.0, 5.0, 3.0, 1.0, -1.0, 3.0, 8.0, 14.0,
                         18.0, 24.0, 29.0, 34.0, 38.0, 42.0, 44.0, 45.0, 43.0, 39.0 };

        int valsCt = 0;

        for(FluxTable table : tables){
            for(FluxRecord rec : table.getRecords()){
                assertThat(rec.getValue()).isEqualTo(vals[valsCt++]);
            }
        }


    }

    @Test
    public void pivotTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"O3\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "  |> keep(columns: [\"_time\", \"_measurement\", \"_field\", \"_value\"])\n" +
                "  |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        Integer[] vals = {157, 167, 172, 175, 176, 174, 172, 171, 169, 170};

        int valsCt = 0;

        for(FluxTable table : tables){

            assertThat(SetupTestSuite.columnsContains(table.getColumns(), "O3")).isTrue();
            for(FluxRecord rec : table.getRecords()){
                assertThat(rec.getValueByKey("O3")).isEqualTo(Long.valueOf(vals[valsCt++]));
            }
        }

    }

    @Test
    public void renameTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm10\")\n" +
                "  |> rename(columns: {label: \"name\", gps: \"coords\", location: \"station\"})",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2); //one for each monitor tag set

        tables.forEach(table -> {

            assertThat(SetupTestSuite.columnsContains(table.getColumns(), "name")).isTrue();
            assertThat(SetupTestSuite.columnsContains(table.getColumns(), "label")).isFalse();
            assertThat(SetupTestSuite.columnsContains(table.getColumns(), "coords")).isTrue();
            assertThat(SetupTestSuite.columnsContains(table.getColumns(), "gps")).isFalse();
            assertThat(SetupTestSuite.columnsContains(table.getColumns(), "station")).isTrue();
            assertThat(SetupTestSuite.columnsContains(table.getColumns(), "location")).isFalse();
            assertThat(SetupTestSuite.columnsContains(table.getColumns(), "city")).isTrue(); //should be unchanged
            assertThat(SetupTestSuite.columnsContains(table.getColumns(), "unitId")).isTrue(); //should be unchanged

        });

    }

    @Test
    public void setTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                "  |> set(key: \"city\", value: \"Prague\")\n" +
                "  |> set(key: \"test\", value: \"pokus\")\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2); //one for each monitor tag set

        tables.forEach(table -> {

            table.getRecords().forEach(rec -> {
                assertThat(rec.getValueByKey("city")).isEqualTo("Prague");
                assertThat(rec.getValueByKey("test")).isEqualTo("pokus");
            });

        });
    }

    @Test
    public void timeShiftTest(){
        String query1 = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"battery-v\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables1 = queryClient.query(query1, SetupTestSuite.getInflux2conf().getOrgId());

        String query2 = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"battery-v\")\n" +
                "  |> timeShift(duration: -1h, columns: [\"_start\", \"_stop\", \"_time\"])",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables2 = queryClient.query(query2, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables1.size()).isEqualTo(2);
        assertThat(tables2.size()).isEqualTo(2);


        for(int i = 0; i < tables1.size(); i++){

            for(int j = 0; j < tables1.get(i).getRecords().size(); j++){

                Instant d1 = (Instant)tables1.get(i).getRecords().get(j).getValueByKey("_time");
                Instant d2 = (Instant)tables2.get(i).getRecords().get(j).getValueByKey("_time");

                assertThat(d1.minusSeconds(3600)).isBetween(d2.minusMillis(100), d2.plusMillis(100));

                d1 = (Instant)tables1.get(i).getRecords().get(j).getValueByKey("_start");
                d2 = (Instant)tables2.get(i).getRecords().get(j).getValueByKey("_start");

                assertThat(d1.minusSeconds(3600)).isBetween(d2.minusMillis(100), d2.plusMillis(100));

                d1 = (Instant)tables1.get(i).getRecords().get(j).getValueByKey("_stop");
                d2 = (Instant)tables2.get(i).getRecords().get(j).getValueByKey("_stop");

                assertThat(d1.minusSeconds(3600)).isBetween(d2.minusMillis(100), d2.plusMillis(100));

            }

        }

    }

    @Test
    public void sortTest(){

        //Asc
        String query1 = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"CO\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "  |> sort(columns: [\"_value\"], desc: false)",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        //Desc
        String query2 = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"CO\")\n" +
                        "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                        "  |> sort(columns: [\"_value\"], desc: true)",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables1 = queryClient.query(query1, SetupTestSuite.getInflux2conf().getOrgId());
        List<FluxTable> tables2 = queryClient.query(query2, SetupTestSuite.getInflux2conf().getOrgId());

        double currVal = Integer.MIN_VALUE;

        for(FluxRecord rec : tables1.get(0).getRecords()){
            assertThat((Double)rec.getValue()).isGreaterThanOrEqualTo(currVal);
            currVal = (Double)rec.getValue();
        }

        currVal = Integer.MAX_VALUE;

        for(FluxRecord rec : tables2.get(0).getRecords()){
            assertThat((Double)rec.getValue()).isLessThanOrEqualTo(currVal);
            currVal = (Double)rec.getValue();
        }

    }

    @Test
    public void unionTest(){

        String query = String.format("hlavni = from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r.location == \"Praha hlavni\")\n" +
                "  |> filter(fn: (r) => r._field == \"battery-v\")\n" +
                "  |> drop(columns: [\"_start\", \"_stop\", \"unitId\", \"label\", \"gps\", \"location\"])\n" +
                "\n" +
                "Smichov = from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "  |> filter(fn: (r) => r._field == \"battery-v\")\n" +
                "  |> drop(columns: [\"_start\", \"_stop\", \"unitId\", \"label\", \"gps\", \"location\"])\n" +
                "\n" +
                "union(tables: [hlavni, Smichov])",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        Double[] valsHlavni = {
                         2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.0,
                         2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0 };
        Double[] valsSmichov = {
                         3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 2.9, 2.9, 2.9, 2.9,
                         2.9, 2.9, 2.9, 2.9, 2.9, 2.9, 2.9, 2.9, 2.9, 2.9
                         };

        Double[] vals;

        int valsCt = 0;


        //union order seems unpredicatble
        if((Double)tables.get(0).getRecords().get(0).getValue() == 2.1){ // Hlavni is first

            vals = Stream.concat(Arrays.stream(valsHlavni), Arrays.stream(valsSmichov)).toArray(Double[]::new);

        }else{ //Hlavni is second

            vals = Stream.concat(Arrays.stream(valsSmichov), Arrays.stream(valsHlavni)).toArray(Double[]::new);

        }

        for(FluxRecord rec : tables.get(0).getRecords()){

            assertThat((Double)rec.getValue()).isEqualTo(vals[valsCt++]);

        }


    }
}
