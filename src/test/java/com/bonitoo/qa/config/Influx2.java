package com.bonitoo.qa.config;

public class Influx2 {

    private String url;
    private String api;
    private String build;

    public String getUrl() {
        return url;
    }

    public String getApi() {
        return api;
    }

    public String getBuild() {
        return build;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setApi(String api) {
        this.api = api;
    }

    public void setBuild(String build) {
        this.build = build;
    }
}
