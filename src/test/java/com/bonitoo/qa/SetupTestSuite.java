package com.bonitoo.qa;

import com.bonitoo.qa.config.TestConfig;
import com.bonitoo.qa.data.AirMonitorRecord;
import com.bonitoo.qa.flux.rest.artifacts.OrganizationArray;
import com.bonitoo.qa.flux.rest.artifacts.authorization.AuthorizationArray;
import com.bonitoo.qa.flux.rest.artifacts.telegraf.*;
import com.bonitoo.qa.influx2.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import de.vandermeer.asciitable.AsciiTable;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import com.influxdb.client.InfluxDBClient;
import org.glassfish.jersey.logging.LoggingFeature;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.OnboardingResponse;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.exceptions.UnprocessableEntityException;
import com.influxdb.query.FluxColumn;
import com.influxdb.query.FluxTable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.ws.rs.client.*;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class SetupTestSuite {

    private static final Logger LOG = LoggerFactory.getLogger(SetupTestSuite.class);


    private static Configuration Influx2conf = new Configuration();
    private static TestConfig TestConf = new TestConfig();
    private static InfluxDBClient InfluxDB;
    private static Client Client;
    private static Map<String, Cookie> Cookies = new HashMap<String, Cookie>();
    private static String OrgId;
    private static String AuthToken;
    private static String[] DEFAULT_INPUT_PLUGINS = {"cpu", "disk", "diskio", "mem",
                                                     "net", "processes", "swap", "system"};


    private static List<AirMonitorRecord> airRecords1;
    private static List<AirMonitorRecord> airRecords2;
    private static long recordInterval = 10 * 60000; // ms - 10 min


    @BeforeClass
    public static void setup() throws Exception {


        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try {

            TestConf = mapper.readValue(new File("testConfig.yml"), TestConfig.class);

        } catch (Exception e) {

            LOG.error(e.getMessage(), e);
            throw(e);

        }


        LOG.info("Starting influx2 docker " + TestConf.getInflux2().getBuild() + " build.");

        CLIWrapper.setDO_SUDO(CLIWrapper.SUDO.SUDO);
        CLIWrapper.setDO_WAIT(CLIWrapper.WAIT.WAIT);

        if(TestConf.getInflux2().getBuild().toUpperCase().contains("ALPHA")){
            //Pull and Start influxd nightly from script alpha release
            CLIWrapper.RUN_CMDX(CLIWrapper.getInflux2Script(), "-a");

        }else{
            //Pull and Start influxd nightly from script
            CLIWrapper.RUN_CMDX(CLIWrapper.getInflux2Script());
        }

        List<String> bucketIDs = new ArrayList<String>();


        try {

            //
            // Do onboarding
            //
            OnboardingResponse response = InfluxDBClientFactory
                    .onBoarding(TestConf.getInflux2().getUrl(),
                            TestConf.getOrg().getAdmin(),
                            TestConf.getOrg().getPassword(),
                            TestConf.getOrg().getName(),
                            TestConf.getOrg().getBucket());

            bucketIDs.add(response.getBucket().getId());
            Influx2conf.setBucketIds(bucketIDs);
            Influx2conf.setOrgId(response.getOrg().getId());
            Influx2conf.setToken(response.getAuth().getToken());

        } catch (UnprocessableEntityException exception) {

            //
            // Onboarding already done
            //
            InfluxDBClient influxDBClient = InfluxDBClientFactory.create(
                    TestConf.getInflux2().getUrl(),
                    TestConf.getOrg().getAdmin(),
                    TestConf.getOrg().getPassword().toCharArray());

            bucketIDs.add(influxDBClient.getBucketsApi().findBuckets().get(0).getId());
            Influx2conf.setBucketIds(bucketIDs);
            //bucketID = platformClient.createBucketClient().findBuckets().get(0).getId();
            Influx2conf.setOrgId(influxDBClient.getOrganizationsApi().findOrganizations().get(0).getId());
            //orgID = platformClient.createOrganizationClient().findOrganizations().get(0).getId();
            Influx2conf.setToken(influxDBClient.getAuthorizationsApi().findAuthorizations().get(0).getToken());
            //token = platformClient.createAuthorizationClient().findAuthorizations().get(0).getToken();

            influxDBClient.close();
        }

        InfluxDB = InfluxDBClientFactory.create(TestConf.getInflux2().getUrl(),
                Influx2conf.getToken().toCharArray());

        signIn();
        fetchOrgId();
        fetchToken();
        Thread.sleep(3000); //troubleshoot why getting invalid org permission when setting up telegraf
        setupTelegraf();
        restartTelegraf();

        //wait 30 sec for telegraf to generate some points
        LOG.info("Waiting 60 sec for telegraf to generate points");
        Thread.sleep(60000);

    }

    public static void setupAirRecords(){

        if(airRecords1 != null && airRecords2 != null){ //already setup
            return;
        }

        airRecords1 = new ArrayList<AirMonitorRecord>();

        airRecords1.add(new AirMonitorRecord(57, 30, 110, 4.3, 59, 48, 10, 68, 1021, 3.1, 269.1, 3.0, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(57, 29, 107, 4.1, 57, 49, 10.1, 68.5, 1020, 3.0, 268.9, 3.0, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(56, 27, 106, 4.1, 55, 49, 10.15, 68, 1020, 2.9, 268.5, 3.0, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(56, 27, 105, 4.0, 54, 50, 10.2, 67.5, 1019, 2.8, 267.0, 3.0, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(55, 27, 103, 3.9, 54, 51, 10.2, 67.5, 1019, 3.0, 265.1, 3.0, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(56, 28, 101, 3.8, 54, 53, 10.25, 67, 1019, 3.0, 262.3, 3.0, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(57, 30, 99,  4.2, 56, 52, 10.3, 66.5, 1019, 3.1, 250.5, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(58, 32, 103, 4.4, 58, 51, 10.4, 66, 1020, 3.1, 248.2, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(60, 32, 108, 4.6, 60, 51, 10.35, 66, 1020, 3.2, 246.0, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(63, 35, 114, 4.9, 61, 51, 10.3, 65.5, 1019, 3.3, 243.9, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(66, 36, 118, 5.4, 63, 55, 10.4, 65.5, 1019, 3.4, 242.1, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(70, 39, 124, 5.8, 60, 55, 10.45, 66, 1020, 3.6, 244.0, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(75, 44, 129, 6.4, 58, 55, 10.5, 66, 1020, 3.3, 247.5, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(81, 50, 134, 7.0, 57, 55, 10.5, 66, 1021, 3.0, 251.0, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(85, 55, 138, 7.7, 60, 57, 10.55, 66.5, 1021, 2.9, 255.3, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(88, 58, 142, 8.2, 62, 57, 10.6, 67, 1020, 2.8, 258.2, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(90, 61, 144, 8.3, 65, 59, 10.6, 67, 1020, 2.7, 260.7, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(89, 61, 145, 8.3, 64, 55, 10.7, 68.5, 1020, 2.6, 262.8, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(89, 58, 143, 8.0, 63, 54, 10.75, 68, 1019, 2.6, 263.0, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));
        airRecords1.add(new AirMonitorRecord(86, 57, 139, 7.8, 63, 52, 10.75, 68, 1019, 2.7, 263.4, 2.9, "50.05.16 14.25.14", "Praha hlavni", "Nadrazi", "Praha", 101));

        airRecords2 = new ArrayList<AirMonitorRecord>();

        airRecords2.add(new AirMonitorRecord(155, 37, 170, 10, 70, 98, 9.2, 74, 1021, 3.1, 269.1, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(157, 39, 175, 12, 74, 97, 9.1, 73.5, 1020, 3.2, 269.0, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(160, 40, 180, 13, 75, 97, 9.1, 73.5, 1020, 3.2, 268.8, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(167, 42, 186, 16, 79, 97, 9.15, 73, 1020, 3.2, 268.5, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(168, 44, 193, 17, 83, 95, 9.15, 72.8, 1019, 3.2, 268.3, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(172, 44, 200, 19, 88, 92, 9.2, 72.6, 1019, 3.3, 268.3, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(174, 48, 207, 21, 87, 90, 9.25, 72.8, 1020, 3.3, 265.8, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(175, 47, 211, 22, 86, 88, 9.25, 72.7, 1020, 3.3, 266.1, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(180, 46, 213, 25, 86, 87, 9.2, 72.5, 1020, 3.3, 266.4, 2.1, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(176, 42, 212, 22, 85, 87, 9.2, 72.4, 1020, 3.3, 266.9, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(175, 40, 217, 22, 85, 87, 9.3, 72.5, 1019, 3.5, 267.0, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(174, 39, 217, 21, 84, 87, 9.3, 72.3, 1019, 3.5, 267.1, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(174, 39, 217, 20, 84, 86, 9.35, 72.2, 1020, 3.5, 268.5, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(172, 38, 214, 18, 84, 85, 9.45, 72.2, 1020, 3.3, 268.7, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(171, 37, 208, 16, 83, 84, 9.45, 72.1, 1021, 3.2, 269.0, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(171, 37, 204, 15, 82, 84, 9.5, 72, 1021, 3.4, 269.8, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(171, 37, 201, 15, 82, 82, 9.5, 72, 1022, 3.5, 270.3, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(169, 38, 202, 14, 82, 83, 9.6, 72, 1023, 3.6, 270.5, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(169, 36, 201, 12, 82, 83, 9.55, 71.8, 1023, 3.8, 270.6, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));
        airRecords2.add(new AirMonitorRecord(170, 37, 198, 12, 81, 83, 9.5, 71.7, 1024, 3.7, 271.2, 2.0, "50.03.41 14.24.32", "Smichov", "Sm Nadrazi", "Praha", 231));

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
                    .addTag("city", rec.getCity())
                    .addField("O3", rec.getO3())
                    .addField("ppm025", rec.getPm025())
                    .addField("ppm10", rec.getPm10())
                    .addField("CO", rec.getCO())
                    .addField("SO2", rec.getSO2())
                    .addField("NO2", rec.getNO2())
                    .addField("temp", rec.getTemp())
                    .addField("humidity", rec.getHumidity())
                    .addField("pressure", rec.getAirpressure())
                    .addField("battery-v", rec.getBatteryVoltage())
                    .time(time += recordInterval, WritePrecision.MS);

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
                        .addTag("city", rec.getCity())
                        //.addField("O3", "") //dropped record - faulty sensor
                        .addField("ppm025", rec.getPm025())
                        .addField("ppm10", rec.getPm10())
                        //.addField("CO", "") //dropped record - faulty sensor
                        //.addField("SO2", "")  //dropped record - faulty sensor
                        //.addField("NO2", "") // dropped record - faulty sensor
                        .addField("temp", rec.getTemp())
                        .addField("humidity", rec.getHumidity())
                        .addField("pressure", rec.getAirpressure())
                        .addField("battery-v", rec.getBatteryVoltage())
                        .time(time += recordInterval, WritePrecision.MS);

            }else{
                p = Point.measurement("air_quality")
                        .addTag("gps", rec.getGpsLocation())
                        .addTag("location", rec.getLocation())
                        .addTag("label", rec.getLabel())
                        .addTag("unitId", Integer.toString(rec.getUnitId()))
                        .addTag("city", rec.getCity())
                        .addField("O3", rec.getO3())
                        .addField("ppm025", rec.getPm025())
                        .addField("ppm10", rec.getPm10())
                        .addField("CO", rec.getCO())
                        .addField("SO2", rec.getSO2())
                        .addField("NO2", rec.getNO2())
                        .addField("temp", rec.getTemp())
                        .addField("humidity", rec.getHumidity())
                        .addField("pressure", rec.getAirpressure())
                        .addField("battery-v", rec.getBatteryVoltage())
                        .time(time += recordInterval, WritePrecision.MS);
            }

            writeClient.writePoint(SetupTestSuite.getInflux2conf().getBucketIds().get(0),
                    SetupTestSuite.getInflux2conf().getOrgId(),
                    p);

            count++;

        }

        writeClient.close();


    }

    public static void signIn(){

        HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(TestConf.getOrg().getAdmin(),
                TestConf.getOrg().getPassword());

        Client = ClientBuilder.newClient( new ClientConfig().register(LoggingFeature.class));
        Client.register(feature);

        WebTarget webTarget = Client.target(TestConf.getInflux2().getUrl() + TestConf.getInflux2().getApi()).path("signin");

        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON);

        Response response = builder.post(Entity.text(""));



        for(String key : response.getCookies().keySet()){
            Cookies.put(key, response.getCookies().get(key));
        }

    }


    public static void fetchOrgId(){

        WebTarget webTarget = Client.target(TestConf.getInflux2APIEndp()).path("orgs");
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON);
        builder.cookie(Cookies.get("session"));

        Response response = builder.get();

//        System.out.println("DEBUG orgs " + response.readEntity(String.class));

        OrganizationArray orgArray = response.readEntity(OrganizationArray.class );

       //should be only 1 org
        OrgId = orgArray.getOrgs().get(0).getId();

    }


    public static void fetchToken(){

        WebTarget webTarget = Client.target(TestConf.getInflux2APIEndp()).path("authorizations");
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON);
        builder.cookie(Cookies.get("session"));

        Response response = builder.get();

        AuthorizationArray auths = response.readEntity(AuthorizationArray.class);

        //Should be only 1 auth
        AuthToken = auths.getAuthorizations().get(0).getToken();

    }


    public static void setupTelegraf() throws IOException{

        WebTarget webTarget = Client.target(TestConf.getInflux2APIEndp()).path("telegrafs");
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON);
        builder.cookie(Cookies.get("session"));

        List<TelegrafPlugin> plugins = new ArrayList<TelegrafPlugin>();

        List<String> influxUrls = new ArrayList<String>();

        influxUrls.add(TestConf.getInflux2().getUrl());

        plugins.add(new TelegrafPluginInflux2Output(
                new TelegrafPluginInflux2Conf(influxUrls, AuthToken, TestConf.getOrg().getName(), TestConf.getOrg().getBucket())));

        for(String s : DEFAULT_INPUT_PLUGINS){
            plugins.add(new TelegrafPluginInput(s));
        }


        TelegrafRequest telegrafReq = new TelegrafRequest(TestConf.getTelegraf().getName(),
                new Agent(),
                OrgId,
                plugins);

 //       ObjectMapper mapper = new ObjectMapper();

//        System.out.println("DEBUG telegraf request: " + mapper.writeValueAsString(telegrafReq));

//        ObjectMapper objectMapper = new ObjectMapper();

//        try {
//            String jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(telegrafReq);
//            System.out.println("DEBUG telegrafReq");
//            System.out.println(jsonString);
//        } catch (JsonProcessingException e) {
//            LOG.warn(e.getMessage(), e);
//        }

        Response r2 = builder.post(Entity.entity(telegrafReq, MediaType.APPLICATION_JSON));

//        System.out.println("DEBUG telegraf response: " + r2.readEntity(String.class));

        Telegraf telegraf = r2.readEntity(Telegraf.class);

//        System.out.println("DEBUG telegraf" + telegraf);

        //Download config

        URL url = new URL(TestConf.getInflux2APIEndp() + "/telegrafs/" + telegraf.getId());
        URLConnection urlConnection = url.openConnection();
        urlConnection.addRequestProperty("Cookie", Cookies.get("session").toString());
        ReadableByteChannel readableByteChannel = Channels.newChannel(urlConnection.getInputStream());
        FileOutputStream fileOutputStream = new FileOutputStream(TestConf.getTelegraf().getConfPath());
        fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);

    }

    public static void restartTelegraf() throws IOException {

        String telegrafStartScript = String.format("#!/bin/sh\n" +
                "\n" +
                "telegraf --config %s > /tmp/telegraf.log 2>&1 &\n", System.getProperty("user.dir") +
                File.separator +
                TestConf.getTelegraf().getConfPath());

        CLIWrapper.WRITE_SCRIPT(telegrafStartScript, "etc/telegraf_start.sh");

        try {
            CLIWrapper.setDO_SUDO(CLIWrapper.SUDO.SUDO);
            CLIWrapper.setDO_WAIT(CLIWrapper.WAIT.WAIT);
            //stop any running telegraf
            CLIWrapper.RUN_CMDX("systemctl", "stop", "telegraf.service");
            CLIWrapper.RUN_CMDX("pkill", "-f", "telegraf");
            //start telegraf using script above
            CLIWrapper.RUN_CMDX("etc/telegraf_start.sh");
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }

    }

    public static void printTables(final String flux, @Nonnull final List<FluxTable> tables){
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

    private static String format(final Object start) {

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

    public static boolean columnsContains(List<FluxColumn> columns, String label){

        for(FluxColumn column : columns){
            if(column.getLabel().equals(label)){
                return true;
            }
        }

        return false;
    }



    /*
      N.B. - if class contains no @Test then static @BeforeClass methods not called
     */

    @Test
    public void something(){
        System.out.println("Empty test case - to trigger class setup");
    }

    public static Configuration getInflux2conf() {
        return Influx2conf;
    }

    public static TestConfig getTestConf() {
        return TestConf;
    }

    public static InfluxDBClient getInfluxDBClient() {
        return InfluxDB;
    }

    public static String getOrgId() {
        return OrgId;
    }

    public static List<AirMonitorRecord> getAirRecords1() {
        return airRecords1;
    }

    public static List<AirMonitorRecord> getAirRecords2() {
        return airRecords2;
    }
}
