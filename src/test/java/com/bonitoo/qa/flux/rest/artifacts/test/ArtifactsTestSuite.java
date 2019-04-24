package com.bonitoo.qa.flux.rest.artifacts.test;

import com.bonitoo.qa.SetupTestSuite;
import com.bonitoo.qa.flux.rest.artifacts.OnBoardRequest;
import de.vandermeer.asciitable.AsciiTable;
import org.influxdata.client.QueryApi;
import org.influxdata.client.WriteApi;
import org.influxdata.client.domain.WritePrecision;
import org.influxdata.client.write.Point;
import org.influxdata.query.FluxTable;
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

/**
 * TODO - review what relavence these tests actually have -
 */


public class ArtifactsTestSuite {

    private static final Logger LOG = LoggerFactory.getLogger(ArtifactsTestSuite.class);

    private static QueryApi queryClient;

    @BeforeClass
    public static void initTest() {

        WriteApi writeClient = SetupTestSuite.getInfluxDBClient().getWriteApi();

//        Instant now = Instant.ofEpochSecond(1548851316);
        Instant now = Instant.now();
        //Long nowMS = System.currentTimeMillis();

        // Mensuration 1
        Point weatherOutdoor1 = Point.measurement("weather_outdoor")
                .addTag("home", "100")
                .addTag("sensor", "120")
                .addField("pressure", 980)
                .addField("wind_speed", 10)
                .addField("precipitation", 860)
                .addField("battery_voltage", 2.6)
                .time(now.minus(60, ChronoUnit.SECONDS), WritePrecision.S);

        writeClient.writePoint(SetupTestSuite.getInflux2conf().getBucketIds().get(0),
                SetupTestSuite.getInflux2conf().getOrgId(),
                weatherOutdoor1);

        // Mensuration 2
        Point weatherOutdoor2 = Point.measurement("weather_outdoor")
                .addTag("home", "100")
                .addTag("sensor", "120")
                .addField("pressure", 860)
                .addField("wind_speed", 12)
                .addField("precipitation", 865)
                .addField("battery_voltage", 2.6)
                .time(now.minus(120, ChronoUnit.SECONDS), WritePrecision.S);

        writeClient.writePoint(SetupTestSuite.getInflux2conf().getBucketIds().get(0),
                SetupTestSuite.getInflux2conf().getOrgId(), weatherOutdoor2);

        // Mensuration 3
        Point weatherOutdoor3 = Point.measurement("weather_outdoor")
                .addTag("home", "100")
                .addTag("sensor", "120")
                .addField("pressure", 880)
                .addField("wind_speed", 11)
                .addField("precipitation", 865)
                .addField("battery_voltage", 2.6)
                .time(now.minus(180, ChronoUnit.SECONDS), WritePrecision.S);

        writeClient.writePoint(SetupTestSuite.getInflux2conf().getBucketIds().get(0),
                SetupTestSuite.getInflux2conf().getOrgId(), weatherOutdoor3);

        writeClient.flush();

        writeClient.close();

        queryClient = SetupTestSuite.getInfluxDBClient().getQueryApi();

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
    public void simpleQuery(){

        // Last Mensuration
        String flux = "from(bucket: \"" + SetupTestSuite.getTestConf().getOrg().getBucket() + "\")\n"
                + "  |> range(start: -10m)\n"
                + "  |> filter(fn: (r) => r._measurement == \"weather_outdoor\")\n"
                + "  |> filter(fn: (r) => r.home == \"100\")\n"
                + "  |> filter(fn: (r) => r.sensor == \"120\")\n"
                + "  |> last()";

        List<FluxTable> tables = queryClient.query(flux, SetupTestSuite.getInflux2conf().getOrgId());

        SetupTestSuite.printTables(flux, tables);

        assertThat(tables.size()).isGreaterThan(0);


    }

}
