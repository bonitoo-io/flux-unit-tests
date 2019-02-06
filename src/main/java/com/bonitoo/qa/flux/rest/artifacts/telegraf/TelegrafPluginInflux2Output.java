package com.bonitoo.qa.flux.rest.artifacts.telegraf;

public class TelegrafPluginInflux2Output extends TelegrafPluginOutput {

    TelegrafPluginInflux2Conf config;

    public TelegrafPluginInflux2Output(TelegrafPluginInflux2Conf config) {
        super();
        this.name = "influxdb_v2";
        this.config = config;
    }

/*
    public TelegrafPluginInflux2Output(String name, TelegrafPluginInflux2Conf config) {
        super(name);
        this.config = config;
    }

    public TelegrafPluginInflux2Output(String name, String comment, TelegrafPluginInflux2Conf config) {
        super(name, comment);
        this.config = config;
    }
*/
    public TelegrafPluginInflux2Conf getConfig() {
        return config;
    }

    public void setConfig(TelegrafPluginInflux2Conf config) {
        this.config = config;
    }
}
