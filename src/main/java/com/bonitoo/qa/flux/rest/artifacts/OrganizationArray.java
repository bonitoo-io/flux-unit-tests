package com.bonitoo.qa.flux.rest.artifacts;

import java.util.List;

public class OrganizationArray {

    private List<Organization> orgs;
    private Links links;


    public List<Organization> getOrgs() {
        return orgs;
    }

    public void setOrgs(List<Organization> orgs) {
        this.orgs = orgs;
    }

    public Links getLinks() {
        return links;
    }

    public void setLinks(Links links) {
        this.links = links;
    }
}
