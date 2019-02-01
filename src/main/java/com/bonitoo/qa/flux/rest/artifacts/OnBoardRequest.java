package com.bonitoo.qa.flux.rest.artifacts;

public class OnBoardRequest {

    private String username;

    private String password;

    private String org;

    private String bucket;

    public OnBoardRequest(String username, String password, String org, String bucket) {
        this.username = username;
        this.password = password;
        this.org = org;
        this.bucket = bucket;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getOrg() {
        return org;
    }

    public String getBucket() {
        return bucket;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setOrg(String org) {
        this.org = org;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }
}
