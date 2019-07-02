package com.bonitoo.qa.functions;

import com.bonitoo.qa.SetupTestSuite;
import org.influxdata.client.QueryApi;
import org.influxdata.client.WriteApi;
import org.influxdata.client.domain.WritePrecision;
import org.influxdata.client.write.Point;
import org.influxdata.query.FluxRecord;
import org.influxdata.query.FluxTable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TypeCastTestSuite {

    private static final Logger LOG = LoggerFactory.getLogger(TypeCastTestSuite.class);
    private static QueryApi queryClient = SetupTestSuite.getInfluxDBClient().getQueryApi();

    private static List<Instant> moments = new ArrayList<Instant>(); //for str-date tests
    private static long DummyRecCount = 100;
    private static long LongInterval = (Long.MAX_VALUE)/(DummyRecCount/2L);
  //  private static double DoubleInterval = (Double.MAX_VALUE)/(DummyRecCount/2.0);

    @BeforeClass
    public static void setup() {

        SetupTestSuite.setupAirRecords();

        long recordInterval = 2 * 60000;
        long time = System.currentTimeMillis() - ((DummyRecCount + 1L) * recordInterval);

        WriteApi writeClientBool = SetupTestSuite.getInfluxDBClient().getWriteApi();
        WriteApi writeClientString = SetupTestSuite.getInfluxDBClient().getWriteApi();

        boolean t = true;
        boolean f = false;
        Double D = Double.MAX_VALUE;

        // TRUE, true, True, t, FALSE, false, False, f

     //   System.out.println("DEBUG Double: " + DoubleInterval + " Dbl.MAX: " + Double.MAX_VALUE + " Dbl.MIN: " + Double.MIN_VALUE );



        for(long i = DummyRecCount; i >= 0; i--){

            Point pBool = Point.measurement("bools")
                    .addTag("boole", "george");

            Point pString = Point.measurement("strings")
                    .addTag("string", "cello");

            if(i % 5 == 0){
                pBool.addField("b", f);
            }else{
                pBool.addField("b",  t);
            }

            String b_lit = "true";

/*            switch(i % 10){
                case 0:
                    b_lit = "TRUE";
                    break;
                case 1:
                    b_lit = "true";
                    break;
                case 2:
                    b_lit = "True";
                    break;
                case 3:
                    b_lit = "t";
                    break;
                case 4:
                    b_lit = "T";
                    break;
                case 5:
                    b_lit = "FALSE";
                    break;
                case 6:
                    b_lit = "false";
                    break;
                case 7:
                    b_lit = "False";
                    break;
                case 8:
                    b_lit = "f";
                    break;
                case 9:
                    b_lit = "F";
                    break;
            } */

            if(i % 2 == 1){
                b_lit = "false";
            }


            pBool.addField("b_lit", b_lit);

            pBool.addField("b_int", (i % 2));

            pString.addField("float_str",  Double.toString(D));

            //manipulate the double bits to get a new value next iter
            //Long L = D.doubleToLongBits(D);
            //long l = L.longValue();
            D = (i < DummyRecCount/2.0)  ? Math.pow((Math.sqrt(i) + 1), i) : 1.0/(Math.pow(DummyRecCount - i,  DummyRecCount - i));
           // D = D.longBitsToDouble(l);

            pString.addField("long_str", Long.toString(Long.MAX_VALUE - (LongInterval * Long.valueOf(i))));

            moments.add(Instant.ofEpochMilli((new Date().getTime() - (i * 10000))));

            pString.addField("date_str", moments.get(moments.size() - 1).toString());

            pBool.time(time, WritePrecision.MS);

            pString.time(time += recordInterval, WritePrecision.MS);

            writeClientBool.writePoint(SetupTestSuite.getInflux2conf().getBucketIds().get(0),
                    SetupTestSuite.getInflux2conf().getOrgId(),
                    pBool);

            writeClientString.writePoint(SetupTestSuite.getInflux2conf().getBucketIds().get(0),
                    SetupTestSuite.getInflux2conf().getOrgId(),
                    pString);

        }

        writeClientBool.close();
        writeClientString.close();

    }

    @Test
    public void LongToStringTest(){

       String query = String.format("from(bucket: \"%s\")\n" +
               "  |> range(start: -4h, stop: now())\n" +
               "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
               "  |> filter(fn: (r) => r._field == \"SO2\")\n" +
               "  |> filter(fn: (r) => r.location == \"Smichov\" )\n" +
               "  |> toString()\n" +
               "  |> map(fn: (r) => ({ _measurement: r._measurement, _field: r._field, _value: \"FOO[\" + r._value + \"]\" }) , mergeKey: true)",
               SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        String[] vals = {"FOO[74]", "FOO[79]", "FOO[88]", "FOO[86]", "FOO[85]", "FOO[84]", "FOO[84]", "FOO[82]", "FOO[82]", "FOO[81]" };

        int valsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat(rec.getValue()).isInstanceOf(String.class);
            assertThat((String)rec.getValue()).isEqualTo(vals[valsCt++]);
        }


    }

    @Test
    public void LongToDoubleTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"SO2\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\" )\n" +
                "  |> toFloat()  \n" +
                "  |> map(fn: (r) => ({ _measurement: r._measurement, _field: r._field, _value: 100.0 + r._value }), mergeKey: true)\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        Double[] vals = {174.0, 179.0, 188.0, 186.0, 185.0, 184.0, 184.0, 182.0, 182.0, 181.0 };

        int valsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat(rec.getValue()).isInstanceOf(Double.class);
            assertThat((Double)rec.getValue()).isEqualTo(vals[valsCt++]);
        }
    }

    @Test
    public void LongToTimeTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"SO2\")\n" +
                "  |> filter(fn: (r) => r.city == \"Praha\")\n" +
                "  |> filter(fn: (r) => r.gps == \"50.03.41 14.24.32\")\n" +
                "  |> toTime()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        List<Instant> vals = new ArrayList<Instant>();

        long[] lvals = {74, 79, 88, 86, 85, 84, 84, 82, 82, 81};

        for(int i = 0; i < lvals.length; i++){
            //vals.add(Instant.ofEpochMilli(lvals[i]));
            vals.add(Instant.parse(String.format("1970-01-01T00:00:00.0000000%2dZ", lvals[i])));
        }

        int valsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat(rec.getValue()).isInstanceOf(Instant.class);
            assertThat((Instant)rec.getValue()).isEqualTo(vals.get(valsCt++));
        }



    }

    @Test
    public void DoubleToStringTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"CO\") \n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "  |> toString()\n" +
                "  |> map(fn: (r) => ({ _measurement: r._measurement, _field: r._field, _value: \"FOO=\" + r._value + \" ppm\" }), mergeKey: true)",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        String[] vals = {"FOO=12 ppm", "FOO=16 ppm", "FOO=19 ppm", "FOO=22 ppm",  "FOO=22 ppm",
                         "FOO=21 ppm", "FOO=18 ppm", "FOO=15 ppm", "FOO=14 ppm",  "FOO=12 ppm"};

        int valsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat(rec.getValue()).isInstanceOf(String.class);
            assertThat((String)rec.getValue()).isEqualTo(vals[valsCt++]);
        }
    }

    @Test
    public void DoubleToLongTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"air_quality\")\n" +
                "  |> filter(fn: (r) => r._field == \"CO\")\n" +
                "  |> filter(fn: (r) => r.location == \"Smichov\")\n" +
                "  |> toInt()  \n" +
                "  |> map(fn: (r) => ({ _measurement: r._measurement, _field: r._field, _value: r._value * 100 }), mergeKey: true)\n",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        long[] vals = {1200, 1600, 1900, 2200, 2200, 2100, 1800, 1500, 1400, 1200};

        int valsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat(rec.getValue()).isInstanceOf(Long.class);
            assertThat(rec.getValue()).isEqualTo(vals[valsCt++]);
        }

    }

    @Test
    public void StringToBoolTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"bools\")\n" +
                "  |> filter(fn: (r) => r._field == \"b_lit\")\n" +
                "  |> toBool()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        int valsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat(rec.getValue()).isInstanceOf(Boolean.class);

            if(valsCt % 2 == 0){
                assertThat(rec.getValue()).isEqualTo(true);
            }else{
                assertThat(rec.getValue()).isEqualTo(false);
            }

            valsCt++;

        }

    }

    @Test
    public void BoolToStringTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"bools\")\n" +
                "  |> filter(fn: (r) => r._field == \"b\")\n" +
                "  |> toString()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        int valsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat(rec.getValue()).isInstanceOf(String.class);

            if(valsCt % 5 == 0){
                assertThat(rec.getValue()).isEqualTo("false");
            }else{
                assertThat(rec.getValue()).isEqualTo("true");
            }

            valsCt++;

        }

    }

    @Test
    public void LongToBoolTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"bools\")\n" +
                "  |> filter(fn: (r) => r._field == \"b_int\")\n" +
                "  |> toBool()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        int valsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat(rec.getValue()).isInstanceOf(Boolean.class);

            if(valsCt % 2 == 0){
                assertThat(rec.getValue()).isEqualTo(false);
            }else{
                assertThat(rec.getValue()).isEqualTo(true);
            }

            valsCt++;

        }

    }

    @Test
    public void BoolToDoubleTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"bools\")\n" +
                "  |> filter(fn: (r) => r._field == \"b\")\n" +
                "  |> toFloat()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        int valsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat(rec.getValue()).isInstanceOf(Double.class);

            if(valsCt % 5 == 0){
                assertThat(rec.getValue()).isEqualTo(0.0);
            }else{
                assertThat(rec.getValue()).isEqualTo(1.0);
            }

            valsCt++;

        }

    }

    @Test
    public void BoolToLongTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                        "  |> range(start: -4h, stop: now())\n" +
                        "  |> filter(fn: (r) => r._measurement == \"bools\")\n" +
                        "  |> filter(fn: (r) => r._field == \"b\")\n" +
                        "  |> toInt()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        int valsCt = 0;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat(rec.getValue()).isInstanceOf(Long.class);

            if(valsCt % 5 == 0){
                assertThat(rec.getValue()).isEqualTo(Long.valueOf(0));
            }else{
                assertThat(rec.getValue()).isEqualTo(Long.valueOf(1));
            }

            valsCt++;

        }



    }

    @Test
    public void TimeToLongTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"bools\")\n" +
                "  |> filter(fn: (r) => r._field == \"b\") \n" +
                "  |> drop(columns: [\"_value\"])  \n" +
                "  |> duplicate(column: \"_time\", as: \"_value\")\n" +
                "  |> set(key: \"_field\", value: \"l_time\")  \n" +
                "  |> toInt()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        tables.get(0).getRecords().forEach(rec -> {

            assertThat(rec.getValue()).isInstanceOf(Long.class);
            // TODO assert values are in range when converted back to time
//            Instant in = Instant.ofEpochMilli((Long)rec.getValue());
        });

    }

    @Test
    public void StringToLongTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -6h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"strings\")\n" +
                "  |> filter(fn: (r) => r._field == \"long_str\")\n" +
                "  |> toInt()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        long recCt = 0L;

        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat(rec.getValue() instanceof Long);
            assertThat(rec.getValue()).isEqualTo(Long.MAX_VALUE - (LongInterval * (DummyRecCount - recCt++)));
        }

    }

    @Test
    public void StringToDoubleTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -6h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"strings\")\n" +
                "  |> filter(fn: (r) => r._field == \"float_str\")\n" +
                "  |> toFloat()",
                SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        long recCt = 100;

        Double D = Double.MAX_VALUE;


        for(FluxRecord rec : tables.get(0).getRecords()){
            assertThat(rec.getValue() instanceof Double);
            assertThat(rec.getValue()).isEqualTo(D);
            // same function used in setup()
            D = (recCt < DummyRecCount/2.0)  ? Math.pow((Math.sqrt(recCt) + 1), recCt) : 1.0/(Math.pow(DummyRecCount - recCt,  DummyRecCount - recCt));
            recCt--;
        }

    }

    @Test
    public void TimeToStringTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"strings\")\n" +
                "  |> filter(fn: (r) => r._field == \"date_str\")\n" +
                "  |> duplicate(column: \"_time\", as: \"foo_time\")\n" +
                "  |> drop(columns: [\"_value\"])\n" +
                "  |> rename(columns: {foo_time: \"_value\"})\n" +
                "  |> toString()", SetupTestSuite.getTestConf().getOrg().getBucket());

        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        tables.get(0).getRecords().forEach(rec -> {
            assertThat(rec.getValue()).isInstanceOf(String.class);
        });

    }

    @Test
    public void StringToTimeTest(){

        String query = String.format("from(bucket: \"%s\")\n" +
                "  |> range(start: -4h, stop: now())\n" +
                "  |> filter(fn: (r) => r._measurement == \"strings\")\n" +
                "  |> filter(fn: (r) => r._field == \"date_str\")\n" +
                "  |> toTime()",
                SetupTestSuite.getTestConf().getOrg().getBucket());


        List<FluxTable> tables = queryClient.query(query, SetupTestSuite.getInflux2conf().getOrgId());

        //for fun and inspection
        SetupTestSuite.printTables(query, tables);

        assertThat(tables.size()).isEqualTo(1); //one for each monitor tag set

        tables.get(0).getRecords().forEach(rec -> {
            assertThat(rec.getValue()).isInstanceOf(Instant.class);
        });

    }






}
