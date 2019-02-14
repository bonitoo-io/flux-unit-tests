package com.bonitoo.qa.functions;

import com.bonitoo.qa.SetupTestSuite;
import com.bonitoo.qa.data.AirMonitorRecord;
import org.influxdata.client.QueryApi;
import org.influxdata.client.WriteApi;
import org.influxdata.client.write.Point;
import org.influxdata.query.FluxColumn;
import org.influxdata.query.FluxRecord;
import org.influxdata.query.FluxTable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FunctionsTestSuite {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionsTestSuite.class);
    private static QueryApi queryClient = SetupTestSuite.getInfluxDBClient().getQueryApi();

    private static List<AirMonitorRecord> airRecords1;
    private static List<AirMonitorRecord> airRecords2;
    private static long recordInterval = 10 * 60000; // ms - 10 min

    @BeforeClass
    public static void setup(){

        airRecords1 = new ArrayList<AirMonitorRecord>();

        airRecords1.add(new AirMonitorRecord(57, 30, 110, 4.3, 59, 48, 10, 68, 1021, 3.1, 269.1, 3.0, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(57, 29, 107, 4.1, 57, 49, 10.1, 68.5, 1020, 3.0, 268.9, 3.0, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(56, 27, 106, 4.1, 55, 49, 10.15, 68, 1020, 2.9, 268.5, 3.0, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(56, 27, 105, 4.0, 54, 50, 10.2, 67.5, 1019, 2.8, 267.0, 3.0, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(55, 27, 103, 3.9, 54, 51, 10.2, 67.5, 1019, 3.0, 265.1, 3.0, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(56, 28, 101, 3.8, 54, 53, 10.25, 67, 1019, 3.0, 262.3, 3.0, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(57, 30, 99,  4.2, 56, 52, 10.3, 66.5, 1019, 3.1, 250.5, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(58, 32, 103, 4.4, 58, 51, 10.4, 66, 1020, 3.1, 248.2, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(60, 32, 108, 4.6, 60, 51, 10.35, 66, 1020, 3.2, 246.0, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(63, 35, 114, 4.9, 61, 51, 10.3, 65.5, 1019, 3.3, 243.9, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(66, 36, 118, 5.4, 63, 55, 10.4, 65.5, 1019, 3.4, 242.1, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(70, 39, 124, 5.8, 60, 55, 10.45, 66, 1020, 3.6, 244.0, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(75, 44, 129, 6.4, 58, 55, 10.5, 66, 1020, 3.3, 247.5, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(81, 50, 134, 7.0, 57, 55, 10.5, 66, 1021, 3.0, 251.0, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(85, 55, 138, 7.7, 60, 57, 10.55, 66.5, 1021, 2.9, 255.3, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(88, 58, 142, 8.2, 62, 57, 10.6, 67, 1020, 2.8, 258.2, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(90, 61, 144, 8.3, 65, 59, 10.6, 67, 1020, 2.7, 260.7, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(89, 61, 145, 8.3, 64, 55, 10.7, 68.5, 1020, 2.6, 262.8, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(89, 58, 143, 8.0, 63, 54, 10.75, 68, 1019, 2.6, 263.0, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));
        airRecords1.add(new AirMonitorRecord(86, 57, 139, 7.8, 63, 52, 10.75, 68, 1019, 2.7, 263.4, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", 101));

        airRecords2 = new ArrayList<AirMonitorRecord>();

        airRecords2.add(new AirMonitorRecord(155, 37, 170, 10, 70, 98, 9.2, 74, 1021, 3.1, 269.1, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(157, 39, 175, 12, 74, 97, 9.1, 73.5, 1020, 3.2, 269.0, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(160, 40, 180, 13, 75, 97, 9.1, 73.5, 1020, 3.2, 268.8, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(167, 42, 186, 16, 79, 97, 9.15, 73, 1020, 3.2, 268.5, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(168, 44, 193, 17, 83, 95, 9.15, 72.8, 1019, 3.2, 268.3, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(172, 44, 200, 19, 88, 92, 9.2, 72.6, 1019, 3.3, 268.3, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(174, 48, 207, 21, 87, 90, 9.25, 72.8, 1020, 3.3, 265.8, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(175, 47, 211, 22, 86, 88, 9.25, 72.7, 1020, 3.3, 266.1, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(180, 46, 213, 25, 86, 87, 9.2, 72.5, 1020, 3.3, 266.4, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(176, 42, 212, 22, 85, 87, 9.2, 72.4, 1020, 3.3, 266.9, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(175, 40, 217, 22, 85, 87, 9.3, 72.5, 1019, 3.5, 267.0, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(174, 39, 217, 21, 84, 87, 9.3, 72.3, 1019, 3.5, 267.1, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(174, 39, 217, 20, 84, 86, 9.35, 72.2, 1020, 3.5, 268.5, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(172, 38, 214, 18, 84, 85, 9.45, 72.2, 1020, 3.3, 268.7, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(171, 37, 208, 16, 83, 84, 9.45, 72.1, 1021, 3.2, 269.0, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(171, 37, 204, 15, 82, 84, 9.5, 72, 1021, 3.4, 269.8, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(171, 37, 201, 15, 82, 82, 9.5, 72, 1022, 3.5, 270.3, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(169, 38, 202, 14, 82, 83, 9.6, 72, 1023, 3.6, 270.5, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(169, 36, 201, 12, 82, 83, 9.55, 71.8, 1023, 3.8, 270.6, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));
        airRecords2.add(new AirMonitorRecord(170, 37, 198, 12, 81, 83, 9.5, 71.7, 1024, 3.7, 271.2, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", 231));

        long startTime;
        long time = startTime = System.currentTimeMillis() - ((airRecords1.size() + 1) * recordInterval);


        WriteApi writeClient = SetupTestSuite.getInfluxDBClient().getWriteApi();
        int count = 0;

        for(AirMonitorRecord rec : airRecords1){
            Point p = Point.measurement("air_quality")
                    .addTag("gps", rec.getGpsLocation())
                    .addTag("location", rec.getLocation())
                    .addTag("label", rec.getLabel())
                    .addTag("unitId", Integer.toString(rec.getUnitId()))
                    .addField("O3", rec.getO3())
                    .addField("ppm025", rec.getPm025())
                    .addField("ppm10", rec.getPm10())
                    .addField("CO", rec.getCO())
                    .addField("SO2", rec.getSO2())
                    .addField("NO2", rec.getNO2())
                    .addField("temp", rec.getTemp())
                    .addField("humidity", rec.getHumidity())
                    .addField("pressure", rec.getAirpressure())
                    .time(time += recordInterval, ChronoUnit.MILLIS);

            writeClient.writePoint(SetupTestSuite.getInflux2conf().getBucketIds().get(0),
                    SetupTestSuite.getInflux2conf().getOrgId(),
                    p);

        }

        time = startTime;
        //drop some records
        for(AirMonitorRecord rec : airRecords2){
            Point p;
            if(count % 2 == 0){
                p = Point.measurement("air_quality")
                        .addTag("gps", rec.getGpsLocation())
                        .addTag("location", rec.getLocation())
                        .addTag("label", rec.getLabel())
                        .addTag("unitId", Integer.toString(rec.getUnitId()))
                        //.addField("O3", "") //dropped record - faulty sensor
                        .addField("ppm025", rec.getPm025())
                        .addField("ppm10", rec.getPm10())
                        //.addField("CO", "") //dropped record - faulty sensor
                        //.addField("SO2", "")  //dropped record - faulty sensor
                        //.addField("NO2", "") // dropped record - faulty sensor
                        .addField("temp", rec.getTemp())
                        .addField("humidity", rec.getHumidity())
                        .addField("pressure", rec.getAirpressure())
                        .time(time += recordInterval, ChronoUnit.MILLIS);

            }else{
                p = Point.measurement("air_quality")
                        .addTag("gps", rec.getGpsLocation())
                        .addTag("location", rec.getLocation())
                        .addTag("label", rec.getLabel())
                        .addTag("unitId", Integer.toString(rec.getUnitId()))
                        .addField("O3", rec.getO3())
                        .addField("ppm025", rec.getPm025())
                        .addField("ppm10", rec.getPm10())
                        .addField("CO", rec.getCO())
                        .addField("SO2", rec.getSO2())
                        .addField("NO2", rec.getNO2())
                        .addField("temp", rec.getTemp())
                        .addField("humidity", rec.getHumidity())
                        .addField("pressure", rec.getAirpressure())
                        .time(time += recordInterval, ChronoUnit.MILLIS);
            }

            writeClient.writePoint(SetupTestSuite.getInflux2conf().getBucketIds().get(0),
                    SetupTestSuite.getInflux2conf().getOrgId(),
                    p);

            count++;

        }

        writeClient.close();

    }

    // aggregateWindow()
    // fill() - basic - not assertion - because not determenistic
    // N.B. the last value from aggregate window is empty - so fill it
    @Test
    public void aggregateWindowFillTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -3h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"temp\")\n" +
                "  |> window(every: 30m, period: 1h, offset: 3h)\n" +
                "  |> aggregateWindow(every: 30m, fn: mean)\n" +
                "  |> fill(column: \"_value\", value: 10.0)",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isEqualTo(2); //one for each monitor tag set

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            int size = table.getRecords().size();
            assertThat(size).isEqualTo(7);
         //   assertThat(((Double)(table.getRecords().get(0).getValue()) == 10.0) || // fill is not deterministic - so commented
         //           (Double)(table.getRecords().get(size - 1).getValue()) == 10.0).isTrue(); // check the fill value is applied either to first or last record

            table.getRecords().forEach(record -> {
                assertThat(record.getField()).isEqualTo("temp");
                assertThat(record.getValue() instanceof Double).isTrue();
                assertThat((Double)record.getValue() >= 9.15 && (Double)record.getValue() <= 10.75).isTrue();
            });
        });

    }

    //Count
    @Test
    public void countTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"SO2\")\n" +
                "  |> count(columns: [\"_value\"])",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isEqualTo(2); //one for each monitor tag set

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);



        tables.forEach(table -> {
            int size = table.getRecords().size();
            assertThat(size).isEqualTo(1);
        });

        assertThat(tables.get(0).getRecords().get(0).getValue()).isEqualTo(Long.valueOf(10));
        assertThat(tables.get(1).getRecords().get(0).getValue()).isEqualTo(Long.valueOf(20));

    }

    //Difference
    @Test
    public void differenceTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"NO2\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "  |> difference(nonNegative: false, columns: [\"_value\"])",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        Long[] diffs = {Long.valueOf(0), Long.valueOf(-5), Long.valueOf(-4), Long.valueOf(-1), Long.valueOf(0),
                Long.valueOf(-2), Long.valueOf(-1), Long.valueOf(-1), Long.valueOf(0)};

        tables.forEach(table -> {
           int diffIndex = 0;
           for(FluxRecord record : table.getRecords()){
               assertThat(record.getValue()).isEqualTo(diffs[diffIndex++]);
           }
        });

    }

    @Test
    public void increaseTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"pressure\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "  |> increase()\n" +
                "  |> yield(name: \"increase\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        Double[] diffs = {0.0, 0.0, 0.0, 0.0, 0.0,
                          1.0, 1.0, 1.0, 1.0, 1.0,
                          1.0, 2.0, 2.0, 3.0, 3.0,
                          4.0, 5.0, 5.0, 6.0};

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            assertThat(table.getRecords().size()).isEqualTo(19);
            int diffIndex = 0;
            for(FluxRecord record : table.getRecords()){
                assertThat(record.getValue()).isEqualTo(diffs[diffIndex++]);
            }
        });

    }

    @Test
    public void integralTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm10\")\n" +
                "  |> integral(unit: 10s, columns: [\"_value\"])",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isEqualTo(2); //one for each monitor tag set

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            int size = table.getRecords().size();
            assertThat(size).isEqualTo(1);
        });

        assertThat(tables.get(0).getRecords().get(0).getValue()).isEqualTo(230520.0);
        assertThat(tables.get(1).getRecords().get(0).getValue()).isEqualTo(137250.0);

    }

    private Double setPrecision(Double d, int scale){
        return BigDecimal.valueOf(d)
                .setScale(scale, RoundingMode.HALF_UP)
                .doubleValue();

    }

    @Test
    public void meanWindowTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"humidity\")\n" +
                "  |> window(every: 1h, period: 1h, offset: 4h)\n" +
                "  |> mean(columns: [\"_value\"])",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isGreaterThanOrEqualTo(8); // can be up to ten depending on minute of hour when test is run
        assertThat(tables.size()).isLessThanOrEqualTo(10); // can be up to ten depending on minute of hour when test is run

        List<Double> humidVals = new ArrayList<Double>();

        airRecords1.forEach(airMonitorRecord ->  {

            humidVals.add(airMonitorRecord.getHumidity());

        });

        airRecords2.forEach(airMonitorRecord ->  {

            humidVals.add(airMonitorRecord.getHumidity());

        });


        Double max = Collections.max(humidVals);
        Double min = Collections.min(humidVals);

        int meansCt = 0;
        for(FluxTable table : tables){


            assertThat(setPrecision((Double)table.getRecords().get(0).getValue(), 5))
                    .isBetween(min, max);

        }

    }

    @Test
    public void meanTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"humidity\")\n" +
                        "  |> mean(columns: [\"_value\"])",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat(tables.get(0).getRecords().get(0).getValue()).isEqualTo(72.53);
        assertThat(tables.get(1).getRecords().get(0).getValue()).isEqualTo(66.95);
    }

    @Test
    public void medianWindowTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r.location == \"Praha hlavni\")\n" +
                "  |> filter(fn: (r) => r._field == \"SO2\")\n" +
                "  |> window(every: 30m, period: 1h, offset: -4h)\n" +
                "  |> toFloat()\n" +
                "  |> median()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isGreaterThanOrEqualTo(8); // can be up to ten depending on minute of hour when test is run
        assertThat(tables.size()).isLessThanOrEqualTo(10); // can be up to ten depending on minute of hour when test is run

        List<Long> so2Vals = new ArrayList<Long>();

        airRecords1.forEach(airMonitorRecord ->  {

            so2Vals.add(airMonitorRecord.getSO2());

        });

        Long max = Collections.max(so2Vals);
        Long min = Collections.min(so2Vals);

        int mediansCt = 0;
        for(FluxTable table : tables){

            assertThat(setPrecision((Double)table.getRecords().get(0).getValue(), 5))
                    .isBetween(min.doubleValue(), max.doubleValue());

        }


    }

    @Test
    public void medianTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r.location == \"Praha hlavni\")\n" +
                        "  |> filter(fn: (r) => r._field == \"SO2\")\n" +
                        "  |> toFloat()\n" +
                        "  |> median()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        assertThat(tables.get(0).getRecords().get(0).getValue()).isEqualTo(59.5);

    }

    @Test
    public void stddevTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"NO2\")\n" +
                "  |> stddev(columns: [\"_value\"])",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat(setPrecision((Double)tables.get(0).getRecords().get(0).getValue(), 3))
                .isEqualTo(setPrecision(5.313504806936128, 3));
        assertThat(setPrecision((Double)tables.get(1).getRecords().get(0).getValue(), 3))
                .isEqualTo(setPrecision(3.017056774233354, 3));

    }

    @Test
    public void spreadTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"O3\")\n" +
                "  |> spread(columns: [\"_value\"])",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat(tables.get(0).getRecords().get(0).getValue())
                .isEqualTo(Long.valueOf(19));
        assertThat(tables.get(1).getRecords().get(0).getValue())
                .isEqualTo(Long.valueOf(35));

    }

    //Transforms

    private static boolean columnsContains(List<FluxColumn> columns, String label){

        for(FluxColumn column : columns){
            if(column.getLabel().equals(label)){
                return true;
            }
        }

        return false;
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
            assertThat(columnsContains(table.getColumns(), "gps")).as("Columns should not contain [gps]").isFalse();
            assertThat(columnsContains(table.getColumns(), "label")).as("Columns should not contain [label]").isFalse();
            assertThat(columnsContains(table.getColumns(), "_value")).as("Columns should contain [_value]").isTrue();
        });

    }


}
