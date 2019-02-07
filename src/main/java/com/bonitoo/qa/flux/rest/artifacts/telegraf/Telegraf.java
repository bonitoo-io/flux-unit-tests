package com.bonitoo.qa.flux.rest.artifacts.telegraf;

import java.util.List;

public class Telegraf {

    String id;
    String organizationID;
    String name;
    Agent agent;
    List<TelegrafPluginIgnoreConf> plugins;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOrganizationID() {
        return organizationID;
    }

    public void setOrganizationID(String organizationID) {
        this.organizationID = organizationID;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Agent getAgent() {
        return agent;
    }

    public void setAgent(Agent agent) {
        this.agent = agent;
    }

    public List<TelegrafPluginIgnoreConf> getPlugins() {
        return plugins;
    }

    public void setPlugins(List<TelegrafPluginIgnoreConf> plugins) {
        this.plugins = plugins;
    }
}
