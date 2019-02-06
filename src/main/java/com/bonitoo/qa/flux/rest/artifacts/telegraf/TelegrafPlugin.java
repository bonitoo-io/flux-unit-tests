package com.bonitoo.qa.flux.rest.artifacts.telegraf;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TelegrafPlugin{

    String name;
    String type;
    String comment;

    public TelegrafPlugin(){

    }

    public TelegrafPlugin(String name) {
        this.name = name;
        this.type = "";
        this.comment = "";
    }

    public TelegrafPlugin(String name, String comment) {
        this.name = name;
        this.type = "";
        this.comment = comment;
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

}
