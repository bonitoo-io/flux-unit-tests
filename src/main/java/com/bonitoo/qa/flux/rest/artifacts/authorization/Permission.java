package com.bonitoo.qa.flux.rest.artifacts.authorization;

public class Permission {

    String action;
    Resource resource;


    public Permission() {
    }

    public Permission(String action, Resource resource) {
        this.action = action;
        this.resource = resource;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Resource getResource() {
        return resource;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }
}
