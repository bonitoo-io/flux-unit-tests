package com.bonitoo.qa.flux.rest.artifacts;

public class Organization {

    private String id;
    private String name;
    private OrganizationLinks links;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public OrganizationLinks getLinks() {
        return links;
    }

    public void setLinks(OrganizationLinks links) {
        this.links = links;
    }
}
