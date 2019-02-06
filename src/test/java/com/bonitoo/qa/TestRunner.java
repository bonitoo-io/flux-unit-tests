package com.bonitoo.qa;

import com.bonitoo.qa.config.TestConfig;
import com.bonitoo.qa.flux.rest.artifacts.Organization;
import com.bonitoo.qa.flux.rest.artifacts.OrganizationArray;
import com.bonitoo.qa.flux.rest.artifacts.test.ArtifactsTest;
import com.bonitoo.qa.influx2.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientRequest;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap;
import org.glassfish.jersey.logging.LoggingFeature;
import org.influxdata.platform.PlatformClient;
import org.influxdata.platform.PlatformClientFactory;
import org.influxdata.platform.domain.OnboardingResponse;
import org.influxdata.platform.error.rest.UnprocessableEntityException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.*;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.glassfish.jersey.client.authentication.HttpAuthenticationFeature.*;

public class TestRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TestRunner.class);


    private static Configuration Influx2conf = new Configuration();
    private static TestConfig TestConf = new TestConfig();
    private static PlatformClient platform;
    private static Client Client;
    private static Map<String, Cookie> Cookies = new HashMap<String, Cookie>();
    private static String OrgId;

    @BeforeClass
    public static void setup() throws Exception {

        System.out.println("DEBUG setting up lowest level class in file system");

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try {

            TestConfig testConfig = mapper.readValue(new File("testConfig.yml"), TestConfig.class);

            TestConf.setOrg(testConfig.getOrg());
            TestConf.setInflux2(testConfig.getInflux2());
            TestConf.setTelegraf(testConfig.getTelegraf());

            System.out.println(ReflectionToStringBuilder.toString(testConfig.getOrg(), ToStringStyle.MULTI_LINE_STYLE));
            System.out.println(ReflectionToStringBuilder.toString(testConfig.getInflux2(), ToStringStyle.MULTI_LINE_STYLE));
            System.out.println(ReflectionToStringBuilder.toString(testConfig.getTelegraf(), ToStringStyle.MULTI_LINE_STYLE));

        } catch (Exception e) {

            // TODO Auto-generated catch block

            e.printStackTrace();

        }


        LOG.info("Starting influx2 docker " + TestConf.getInflux2().getBuild() + " build.");

        if(TestConf.getInflux2().getBuild().toUpperCase().contains("ALPHA")){
            //Pull and Start influxd nightly from script alpha release
            CLIWrapper.RUN_CMDX(CLIWrapper.SUDO.SUDO,
                    CLIWrapper.WAIT.WAIT,
                    CLIWrapper.getInflux2Script(), "-a");

        }else{
            //Pull and Start influxd nightly from script
            CLIWrapper.RUN_CMDX(CLIWrapper.SUDO.SUDO,
                    CLIWrapper.WAIT.WAIT,
                    CLIWrapper.getInflux2Script());
        }

        List<String> bucketIDs = new ArrayList<String>();

        try {

            //
            // Do onboarding
            //
            OnboardingResponse response = PlatformClientFactory
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
            PlatformClient platformClient = PlatformClientFactory.create(
                    TestConf.getInflux2().getUrl(),
                    TestConf.getOrg().getAdmin(),
                    TestConf.getOrg().getPassword().toCharArray());

            bucketIDs.add(platformClient.createBucketClient().findBuckets().get(0).getId());
            Influx2conf.setBucketIds(bucketIDs);
            //bucketID = platformClient.createBucketClient().findBuckets().get(0).getId();
            Influx2conf.setOrgId(platformClient.createOrganizationClient().findOrganizations().get(0).getId());
            //orgID = platformClient.createOrganizationClient().findOrganizations().get(0).getId();
            Influx2conf.setToken(platformClient.createAuthorizationClient().findAuthorizations().get(0).getToken());
            //token = platformClient.createAuthorizationClient().findAuthorizations().get(0).getToken();

            platformClient.close();
        }

        platform = PlatformClientFactory.create(TestConf.getInflux2().getUrl(),
                Influx2conf.getToken().toCharArray());


    }

    @Test
    public void signIn(){

        HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(TestConf.getOrg().getAdmin(),
                TestConf.getOrg().getPassword());

        Client = ClientBuilder.newClient( new ClientConfig().register(LoggingFeature.class));
        Client.register(feature);

        WebTarget webTarget = Client.target(TestConf.getInflux2().getUrl() + TestConf.getInflux2().getApi()).path("signin");

        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON);

        Response response = builder.post(Entity.text(""));


        System.out.println("DEBUG jersey");

        System.out.println( response.getStatus() );
        System.out.println( response.readEntity(String.class) );
        System.out.println( "cookies");

        for(String key : response.getCookies().keySet()){
            Cookies.put(key, response.getCookies().get(key));
            System.out.println( key + ": " + response.getCookies().get(key));
        }

        System.out.println("DEBUG cookie jar");
        for(String k : Cookies.keySet()){
            System.out.println("   " + k + ": " + Cookies.get(k));
        }

    }

    @Test()
    public void fetchOrgId(){

        WebTarget webTarget = Client.target(TestConf.getInflux2APIEndp()).path("orgs");
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON);
        builder.cookie(Cookies.get("session"));

        Response response = builder.get();

        OrganizationArray orgArray = response.readEntity(OrganizationArray.class );

        System.out.println("DEBUG orgArray");

        //System.out.println(response.readEntity(String.class));

       for(Organization o : orgArray.getOrgs()){
            System.out.println(o.getId() + " " + o.getName());
        }

       //should be only 1 org
        OrgId = orgArray.getOrgs().get(0).getId();

    }

    @Test
    public void setupTelegraf(){

        WebTarget webTarget = Client.target(TestConf.getInflux2APIEndp()).path("telegrafs");
        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON);
        builder.cookie(Cookies.get("session"));

        //TODO - this is just prelim check of cookie - next need to put TelegrafRequest
        Response response = builder.get();

        System.out.println("DEBUG endp telegrafs");

        System.out.println( response.getStatus() );
        System.out.println( response.readEntity(String.class) );

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

    public static PlatformClient getPlatform() {
        return platform;
    }

    public static String getOrgId() {
        return OrgId;
    }
}
