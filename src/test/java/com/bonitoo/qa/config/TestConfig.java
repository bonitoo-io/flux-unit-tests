package com.bonitoo.qa.config;

public class TestConfig {

    Org org = new Org();
    Influx2 influx2 = new Influx2();
    Telegraf telegraf = new Telegraf();

    public Org getOrg() {
        return org;
    }

    public Influx2 getInflux2() {
        return influx2;
    }

    public Telegraf getTelegraf() {
        return telegraf;
    }

    public void setOrg(Org org) {
        this.org = org;
    }

    public void setInflux2(Influx2 influx2) {
        this.influx2 = influx2;
    }

    public void setTelegraf(Telegraf telegraf) {
        this.telegraf = telegraf;
    }

    public String getInflux2APIEndp(){
        return influx2.getUrl() + influx2.getApi();
    }
}
