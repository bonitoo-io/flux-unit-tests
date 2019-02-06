package com.bonitoo.qa.flux.rest.artifacts.authorization;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public class AuthorizationArray {

    List<String> links;

    List<Authorization> authorizations;

    public List<Authorization> getAuthorizations() {
        return authorizations;
    }

    public void setAuthorizations(List<Authorization> authorizations) {
        this.authorizations = authorizations;
    }

    @JsonIgnore
    public List<String> getLinks() {
        return links;
    }

    @JsonIgnore
    public void setLinks(List<String> links) {
        this.links = links;
    }
}
