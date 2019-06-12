package com.bonitoo.qa.flux.rest.artifacts.telegraf;

import java.util.List;

public class TelegrafRequest {

    private String name;
    private Agent agent;
    private String orgID;
    private List<TelegrafPlugin> plugins;

    public TelegrafRequest(String name, Agent agent, String organizationID, List<TelegrafPlugin> plugins) {
        this.name = name;
        this.agent = agent;
        this.orgID = organizationID;
        this.plugins = plugins;
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

    public String getOrgID() {
        return orgID;
    }

    public void setOrgID(String orgID) {
        this.orgID = orgID;
    }


    public List<TelegrafPlugin> getPlugins() {
        return plugins;
    }

    public void setPlugins(List<TelegrafPlugin> plugins) {
        this.plugins = plugins;
    }
}
