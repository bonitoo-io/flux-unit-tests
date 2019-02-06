package com.bonitoo.qa.flux.rest.artifacts.telegraf;

import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.List;

public class TelegrafRequest {

    private String name;
    private String organizationID;
    private List<TelegrafPlugin> plugins;

    public TelegrafRequest(String name, String organizationID, List<TelegrafPlugin> plugins) {
        this.name = name;
        this.organizationID = organizationID;
        this.plugins = plugins;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOrganizationID() {
        return organizationID;
    }

    public void setOrganizationID(String organizationID) {
        this.organizationID = organizationID;
    }


    public List<TelegrafPlugin> getPlugins() {
        return plugins;
    }

    public void setPlugins(List<TelegrafPlugin> plugins) {
        this.plugins = plugins;
    }
}
