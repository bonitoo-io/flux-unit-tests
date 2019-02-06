package com.bonitoo.qa.flux.rest.artifacts.authorization;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public class Authorization {

    List<String> links;
    String id;
    String token;
    String status;
    String description;
    String orgID;
    String org;
    String userID;
    String user;
    List<Permission> permissions;

    @JsonIgnore
    public List<String> getLinks() {
        return links;
    }

    @JsonIgnore
    public void setLinks(List<String> links) {
        this.links = links;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getOrgID() {
        return orgID;
    }

    public void setOrgID(String orgId) {
        this.orgID = orgId;
    }

    public String getOrg() {
        return org;
    }

    public void setOrg(String org) {
        this.org = org;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userId) {
        this.userID = userId;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public List<Permission> getPermissions() {
        return permissions;
    }

    public void setPermissions(List<Permission> permissions) {
        this.permissions = permissions;
    }
}
