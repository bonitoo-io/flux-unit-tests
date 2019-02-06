package com.bonitoo.qa.flux.rest.artifacts.telegraf;

/*
{"name":"influxdb_v2",
        "type":"output",
        "config":{
             "urls":["http://127.0.0.1:9999"],
             "token":"VvgYWj716AoHjbdqyWKXEWSNh3FLn52lFOrbbL1AxSXg5EsIvdqSnxBez1F3vxtjiEmQyXk4q8E-DmmLgu7v_w==",
             "organization":"qa",
              "bucket":"test-data"
        }
}

*/

import java.util.List;

public class TelegrafPluginInflux2Conf {

    List<String> urls;
    String token;
    String organization;
    String bucket;

    public TelegrafPluginInflux2Conf(List<String> urls, String token, String organization, String bucket) {
        this.urls = urls;
        this.token = token;
        this.organization = organization;
        this.bucket = bucket;
    }

    public List<String> getUrls() {
        return urls;
    }

    public void setUrls(List<String> urls) {
        this.urls = urls;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }
}
