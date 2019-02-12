package com.bonitoo.qa;

import com.bonitoo.qa.config.TestConfig;
import com.bonitoo.qa.flux.rest.artifacts.OrganizationArray;
import com.bonitoo.qa.flux.rest.artifacts.authorization.AuthorizationArray;
import com.bonitoo.qa.flux.rest.artifacts.telegraf.*;
import com.bonitoo.qa.influx2.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import de.vandermeer.asciitable.AsciiTable;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.influxdata.client.InfluxDBClient;
import org.glassfish.jersey.logging.LoggingFeature;
import org.influxdata.client.InfluxDBClientFactory;
import org.influxdata.client.domain.OnboardingResponse;
import org.influxdata.exceptions.UnprocessableEntityException;
import org.influxdata.query.FluxTable;
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
            Influx2conf.setOrgId(response.getOrganization().getId());
            Influx2conf.setToken(response.getAuthorization().getToken());

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
        setupTelegraf();
        restartTelegraf();

        //wait 30 sec for telegraf to generate some points
        LOG.info("Waiting 60 sec for telegraf to generate points");
        Thread.sleep(60000);

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

//        ObjectMapper objectMapper = new ObjectMapper();

//        try {
//            String jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(telegrafReq);
//            System.out.println("DEBUG telegrafReq");
//            System.out.println(jsonString);
//        } catch (JsonProcessingException e) {
//            LOG.warn(e.getMessage(), e);
//        }

        Response r2 = builder.post(Entity.entity(telegrafReq, MediaType.APPLICATION_JSON));

        Telegraf telegraf = r2.readEntity(Telegraf.class);

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



    @Test
    public void something(){
        System.out.println("Something test");
        assertThat(true).isEqualTo(true);
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
}
