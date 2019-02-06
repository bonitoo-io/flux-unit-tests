package com.bonitoo.qa.flux.rest.artifacts.telegraf;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TelegrafPlugin {

    private String name;
    private String type;
    private String comment;
    private String config; //should be typed but each class can be different - ignore for now

    public TelegrafPlugin(String name, String type ) {
        this.name = name;
        this.type = type;
        this.comment = "";
        this.config = "";
    }


    public TelegrafPlugin(String name, String type, String comment, String config) {
        this.name = name;
        this.type = type;
        this.comment = comment;
        this.config = config;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @JsonIgnore
    public String getConfig() {
        return config;
    }

    @JsonIgnore
    public void setConfig(String config) {
        this.config = config;
    }
}
