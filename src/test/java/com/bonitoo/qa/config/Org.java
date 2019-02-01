package com.bonitoo.qa.config;

public class Org {

    private String name; //orgname
    private String admin;  //admin user name
    private String password; //admin password
    private String bucket; //default bucket

    public String getName() {
        return name;
    }

    public String getAdmin() {
        return admin;
    }

    public String getPassword() {
        return password;
    }

    public String getBucket() {
        return bucket;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAdmin(String admin) {
        this.admin = admin;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setBacket(String backet) {
        this.bucket = backet;
    }
}
