package com.bonitoo.qa.flux.rest.artifacts.telegraf;

public class TelegrafPluginInput extends TelegrafPlugin {

    TelegrafInputConf config;

    public TelegrafPluginInput(String name){
        super(name);
        this.type = "input";
        this.config = new TelegrafInputConf();
    }

    public TelegrafPluginInput(String name, String config) {
        super(name);
        this.type = "input";
        this.config = new TelegrafInputConf();
    }

    public TelegrafPluginInput(String name, String comment, String config) {
        super(name, comment);
        this.type = "input";
        this.config = new TelegrafInputConf();
    }

    public Object getConfig() {
        return config;
    }

    public void setConfig(TelegrafInputConf config) {
        this.config = config;
    }
}
