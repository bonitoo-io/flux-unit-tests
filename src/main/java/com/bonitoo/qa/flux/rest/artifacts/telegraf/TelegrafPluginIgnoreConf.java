package com.bonitoo.qa.flux.rest.artifacts.telegraf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/*
  Primarily for deserialization -
  Ignore for now
 */
public class TelegrafPluginIgnoreConf extends TelegrafPlugin {

    Object config;

    @JsonIgnore
    public Object getConfig() {
        return null;
    }

    @JsonIgnore
    public void setConfig(Object config) {
        if( config instanceof String)
        this.config = config;
    }
}
