package com.bonitoo.qa.functions;

import com.bonitoo.qa.SetupTestSuite;
import com.bonitoo.qa.data.AirMonitorRecord;
import com.bonitoo.qa.influx2.FluxUtils;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AggregatorsTestSuite {

    private static final Logger LOG = LoggerFactory.getLogger(AggregatorsTestSuite.class);
    private static QueryApi queryClient = SetupTestSuite.getInfluxDBClient().getQueryApi();

    @BeforeClass
    public static void setup(){

        SetupTestSuite.setupAirRecords();

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
                "  |> count(column: \"_value\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isEqualTo(2); //one for each monitor tag set

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);



        tables.forEach(table -> {
            int size = table.getRecords().size();
            assertThat(size).isEqualTo(1);
        });


        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 10L)).isTrue();
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 20L)).isTrue();
        /*
        assertThat(tables.get(0).getRecords().get(0).getValue()).isEqualTo(Long.valueOf(10));
        assertThat(tables.get(1).getRecords().get(0).getValue()).isEqualTo(Long.valueOf(20));
        */

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
                "  |> integral(unit: 10s, column: \"_value\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        assertThat(tables.size()).isEqualTo(2); //one for each monitor tag set

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        tables.forEach(table -> {
            int size = table.getRecords().size();
            assertThat(size).isEqualTo(1);
        });

        //tables order is not always deterministic
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 230520.0)).isTrue();
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 137250.0)).isTrue();

        /*
        assertThat(tables.get(0).getRecords().get(0).getValue()).isEqualTo(230520.0);
        assertThat(tables.get(1).getRecords().get(0).getValue()).isEqualTo(137250.0);
        */

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
                "  |> mean(column: \"_value\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isGreaterThanOrEqualTo(8); // can be up to ten depending on minute of hour when test is run
        assertThat(tables.size()).isLessThanOrEqualTo(10); // can be up to ten depending on minute of hour when test is run

        List<Double> humidVals = new ArrayList<Double>();

        SetupTestSuite.getAirRecords1().forEach(airMonitorRecord ->  {

            humidVals.add(airMonitorRecord.getHumidity());

        });

        SetupTestSuite.getAirRecords2().forEach(airMonitorRecord ->  {

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
                        "  |> mean(column: \"_value\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 72.53)).isTrue();
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 66.95)).isTrue();

        /*
        assertThat(tables.get(0).getRecords().get(0).getValue()).isEqualTo(72.53);
        assertThat(tables.get(1).getRecords().get(0).getValue()).isEqualTo(66.95);
        */
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

        SetupTestSuite.getAirRecords1().forEach(airMonitorRecord ->  {

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
                "  |> stddev(column: \"_value\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 5.31350)).isTrue();
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 3.01705)).isTrue();

/*
        assertThat(setPrecision((Double)tables.get(0).getRecords().get(0).getValue(), 3))
                .isEqualTo(setPrecision(5.313504806936128, 3));
        assertThat(setPrecision((Double)tables.get(1).getRecords().get(0).getValue(), 3))
                .isEqualTo(setPrecision(3.017056774233354, 3));
                */

    }

    @Test
    public void spreadTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"O3\")\n" +
                "  |> spread(column: \"_value\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 19L)).isTrue();
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 35L)).isTrue();

/*
        assertThat(tables.get(0).getRecords().get(0).getValue())
                .isEqualTo(Long.valueOf(19));
        assertThat(tables.get(1).getRecords().get(0).getValue())
                .isEqualTo(Long.valueOf(35));
                */

    }

    @Test
    public void sumTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm10\")\n" +
                "  |> sum(column: \"_value\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        //table order not always deterministitc
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables,4026.0)).isTrue();
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables,2412.0)).isTrue();

/*
        assertThat(tables.get(0).getRecords().get(0).getValue())
                .isEqualTo(4026.0);
        assertThat(tables.get(1).getRecords().get(0).getValue())
                .isEqualTo(2412.0);
                */
    }

    @Test
    public void sumWindowTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                "  |> window(every: 30m, period: 30m, offset: -4h)\n" +
                "  |> sum(column: \"_value\")",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        List<Double>ppm025s = new ArrayList<Double>();

        for(AirMonitorRecord rec : SetupTestSuite.getAirRecords2()){
            ppm025s.add(rec.getPm025());
        }

        Double maxPpm025 = Collections.max(ppm025s);
        Double minPpm025 = Collections.min(ppm025s);

        assertThat(tables.size()).isBetween(7,8); //depending on minute of hour

        tables.forEach(table -> {
           if(!(table.equals(tables.get(0)) || table.equals(tables.get(tables.size() - 1)))){
               assertThat((Double)table.getRecords().get(0).getValue())
                       .isBetween(minPpm025 * 3, maxPpm025 * 3); // each window except for first and last should have 3 data pts
           }
        });

    }

    @Test
    public void quantileTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                "  |> quantile(q: 0.99, method: \"estimate_tdigest\", compression: 1000.0 )\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 48.0)).isTrue();
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 61.0)).isTrue();

        /*
        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(48.0);
        assertThat((Double)tables.get(1).getRecords().get(0).getValue()).isEqualTo(61.0);
        */
    }

    @Test
    public void skewTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                        "  |> skew()\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 0.77953));
        assertThat(FluxUtils.tablesContainsRecordWithVal(tables, 0.44498));

        /*
        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(0.7795347338130559);
        assertThat((Double)tables.get(1).getRecords().get(0).getValue()).isEqualTo(0.44497585268630263);
        */

    }

    @Test
    public void pearsonrTest(){

        String query = String.format("smichov = from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "\n" +
                "hlavni = from(bucket: \"%s\")\n" +
                "    |> range(start: -4h)\n" +
                "    |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "    |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                "    |> filter(fn: (r) => r.location == \"Praha hlavni\")\n" +
                "\n" +
                "pearsonr(x: smichov, y: hlavni, on: [\"_time\", \"_field\"])\n",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getOrg().getBucket());


        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(-0.6881492738206025);


    }

    @Test
    public void covTest(){

        String query = String.format("smichov = from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h)\n" +
                        "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "  |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                        "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                        "\n" +
                        "hlavni = from(bucket: \"%s\")\n" +
                        "    |> range(start: -4h)\n" +
                        "    |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                        "    |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                        "    |> filter(fn: (r) => r.location == \"Praha hlavni\")\n" +
                        "\n" +
                        "cov(x: smichov, y: hlavni, on: [\"_time\", \"_field\"])\n",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getOrg().getBucket());


        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(-33.39999999999998);

    }

    @Test
    public void covarianceTest(){

        String query = String.format("smichov = from(bucket: \"%s\")\n" +
                "  |> range(start: -4h)\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "\n" +
                "hlavni = from(bucket: \"%s\")\n" +
                "    |> range(start: -4h)\n" +
                "    |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "    |> filter(fn: (r) => r._field == \"ppm025\")\n" +
                "    |> filter(fn: (r) => r.location == \"Praha hlavni\")\n" +
                "\n" +
                "join(tables: {key1: smichov, key2: hlavni}, on: [\"city\", \"_field\", \"_measurement\", \"_start\", \"_stop\", \"_time\"], method: \"inner\")\n" +
                "   |> covariance(columns: [\"_value_key1\",\"_value_key2\"], pearsonr: false, valueDst: \"_value\")\n",
                SetupTestSuite.getTestConf().getOrg().getBucket(),
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1);

        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(-33.39999999999998);

    }

    @Test
    public void histogramQuantileTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"CO\")\n" +
                "  |> histogram(column: \"_value\", upperBoundColumn: \"le\", countColumn: \"_value\", bins: [5.0, 10.0, 15.0, 20.0, 25.0 ], normalize: false)\n" +
                "  |> histogramQuantile(quantile: 0.9, countColumn: \"_value\", upperBoundColumn: \"le\", valueColumn: \"_value\", minValue: 0.0)",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(2);

        assertThat((Double)tables.get(0).getRecords().get(0).getValue()).isEqualTo(23.333333333333332);
        assertThat((Double)tables.get(1).getRecords().get(0).getValue()).isEqualTo(9.0);


    }


    //Transforms




}
