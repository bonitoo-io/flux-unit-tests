package com.bonitoo.qa.flux.rest.artifacts.test;

import com.bonitoo.qa.TestRunner;
import com.bonitoo.qa.flux.rest.artifacts.OnBoardRequest;
import de.vandermeer.asciitable.AsciiTable;
import org.influxdata.flux.domain.FluxTable;
import org.influxdata.platform.QueryClient;
import org.influxdata.platform.WriteClient;
import org.influxdata.platform.write.Point;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ArtifactsTest {

    private static final Logger LOG = LoggerFactory.getLogger(ArtifactsTest.class);


    private static OnBoardRequest onbReq;
    private static QueryClient queryClient;


    @BeforeClass
    public static void initTest() {

        onbReq = new OnBoardRequest("admin", "changeit", "qa", "test-data");

        WriteClient writeClient = TestRunner.getPlatform().createWriteClient();

        Instant now = Instant.ofEpochSecond(1548851316);

        // Mensuration 1
        Point weatherOutdoor1 = Point.measurement("weather_outdoor")
                .addTag("home", "100")
                .addTag("sensor", "120")
                .addField("pressure", 980)
                .addField("wind_speed", 10)
                .addField("precipitation", 860)
                .addField("battery_voltage", 2.6)
                .time(now, ChronoUnit.SECONDS);

        writeClient.writePoint(TestRunner.getInflux2conf().getBucketIds().get(0),
                TestRunner.getInflux2conf().getOrgId(),
                weatherOutdoor1);

        // Mensuration 2
        Point weatherOutdoor2 = Point.measurement("weather_outdoor")
                .addTag("home", "100")
                .addTag("sensor", "120")
                .addField("pressure", 860)
                .addField("wind_speed", 12)
                .addField("precipitation", 865)
                .addField("battery_voltage", 2.6)
                .time(now.plus(10, ChronoUnit.SECONDS), ChronoUnit.SECONDS);

        writeClient.writePoint(TestRunner.getInflux2conf().getBucketIds().get(0),
                TestRunner.getInflux2conf().getOrgId(), weatherOutdoor2);

        // Mensuration 3
        Point weatherOutdoor3 = Point.measurement("weather_outdoor")
                .addTag("home", "100")
                .addTag("sensor", "120")
                .addField("pressure", 880)
                .addField("wind_speed", 11)
                .addField("precipitation", 865)
                .addField("battery_voltage", 2.6)
                .time(now.plus(20, ChronoUnit.SECONDS), ChronoUnit.SECONDS);

        writeClient.writePoint(TestRunner.getInflux2conf().getBucketIds().get(0),
                TestRunner.getInflux2conf().getOrgId(), weatherOutdoor3);

        writeClient.close();

        queryClient = TestRunner.getPlatform().createQueryClient();

    }

    @Before
    public void beforeEachTest() {
        //System.out.println("This is executed before each Test");
    }

    @After
    public void afterEachTest() {
        //System.out.println("This is exceuted after each Test");
    }

    @Test
    public void testOnboard() {

        assertThat(onbReq.getUsername()).isEqualTo("admin");
        assertThat(onbReq.getPassword()).isEqualTo("changeit");
        assertThat(onbReq.getOrg()).isEqualTo("qa");
        assertThat(onbReq.getBucket()).isEqualTo("test-data");
        //assertEquals(7, result);
    }

    @Test
    public void testConfig(){

        System.out.println("Orgname: " + TestRunner.getTestConf().getOrg().getName());
        System.out.println("Admin: " + TestRunner.getTestConf().getOrg().getAdmin());
        System.out.println("Password: " + TestRunner.getTestConf().getOrg().getPassword());
        System.out.println("Bucket: " + TestRunner.getTestConf().getOrg().getBucket());

        System.out.println("Influx2 URL: " + TestRunner.getTestConf().getInflux2().getUrl());
        System.out.println("Influx2 API: " + TestRunner.getTestConf().getInflux2().getApi());

    }

    @Test
    public void testInfluxsConfig(){
        System.out.println("OrgId: " + TestRunner.getInflux2conf().getOrgId());
        System.out.println("First Bucket: " + TestRunner.getInflux2conf().getBucketIds().get(0));
        System.out.println("Token: " + TestRunner.getInflux2conf().getToken());
    }

    @Test
    public void simpleQuery(){

        // Last Mensuration
        String flux = "from(bucket: \"" + TestRunner.getTestConf().getOrg().getBucket() + "\")\n"
                + "  |> range(start: 0)\n"
                + "  |> filter(fn: (r) => r._measurement == \"weather_outdoor\")\n"
                + "  |> filter(fn: (r) => r.home == \"100\")\n"
                + "  |> filter(fn: (r) => r.sensor == \"120\")\n"
                + "  |> last()";

        List<FluxTable> tables = queryClient.query(flux, TestRunner.getInflux2conf().getOrgId());

        printResult(flux, tables);

    }

    //because I haven't done this in a while
    @Test
    public void testFail(){
        assertThat(true).isFalse();
    }


    @Ignore
    @Test
    public void testFalse() {

        assertThat(false).isFalse();
    }

    @Ignore
    @Test
    public void testSubstraction() {
        int result = 10 - 3;

        assertThat(result).isEqualTo(9);
    }

    private void printResult(final String flux, @Nonnull final List<FluxTable> tables) {

        AsciiTable at = new AsciiTable();

        at.addRule();
        at.addRow("table", "_start", "_stop", "_time", "_measurement", "_field", "_value");
        at.addRule();

        tables.forEach(table -> table.getRecords().forEach(record -> {

            String tableIndex = format(record.getTable());
            String start = format(record.getStart());
            String stop = format(record.getStop());
            String time = format(record.getTime());

            String measurement = format(record.getMeasurement());
            String field = format(record.getField());
            String value = format(record.getValue());

            at.addRow(tableIndex, start, stop, time, measurement, field, value);
            at.addRule();
        }));

        LOG.info("\n\nQuery:\n\n{}\n\nResult:\n\n{}\n", flux, at.render(150));
    }

    private String format(final Object start) {

        if (start == null) {
            return "";
        }

        if (start instanceof Instant) {

            return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(ZoneId.systemDefault())
                    .format((Instant) start);
        }

        return start.toString();
    }

}
