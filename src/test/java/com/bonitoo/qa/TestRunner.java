package com.bonitoo.qa;

import com.bonitoo.qa.config.TestConfig;
import com.bonitoo.qa.influx2.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.influxdata.platform.PlatformClient;
import org.influxdata.platform.PlatformClientFactory;
import org.influxdata.platform.domain.OnboardingResponse;
import org.influxdata.platform.error.rest.UnprocessableEntityException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestRunner {

    private static Configuration Influx2conf = new Configuration();
    private static TestConfig TestConf = new TestConfig();
    private static PlatformClient platform;

    @BeforeClass
    public static void setup() throws Exception {

        System.out.println("DEBUG setting up lowest level class in file system");

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try {

            TestConfig testConfig = mapper.readValue(new File("testConfig.yml"), TestConfig.class);

            TestConf.setOrg(testConfig.getOrg());
            TestConf.setInflux2(testConfig.getInflux2());

            System.out.println(ReflectionToStringBuilder.toString(testConfig.getOrg(), ToStringStyle.MULTI_LINE_STYLE));
            System.out.println(ReflectionToStringBuilder.toString(testConfig.getInflux2(), ToStringStyle.MULTI_LINE_STYLE));

        } catch (Exception e) {

            // TODO Auto-generated catch block

            e.printStackTrace();

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
}
