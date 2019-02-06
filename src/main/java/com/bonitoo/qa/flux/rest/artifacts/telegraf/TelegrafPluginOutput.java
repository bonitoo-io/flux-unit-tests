package com.bonitoo.qa.flux.rest.artifacts.telegraf;

public class TelegrafPluginOutput extends TelegrafPlugin {

    public TelegrafPluginOutput() {
        this.type = "output";
    }

    public TelegrafPluginOutput(String name) {
        super(name);
        this.type = "output";
    }

    public TelegrafPluginOutput(String name, String comment) {
        super(name, comment);
        this.type = "output";
    }
}
