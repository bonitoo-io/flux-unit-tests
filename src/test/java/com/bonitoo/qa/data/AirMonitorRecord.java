package com.bonitoo.qa.data;

public class AirMonitorRecord {

    private long O3; // ozone ppb
    private double pm025; //particulate matter micrograms/m3 size 2.5 micro meters or less
    private double pm10; // particulate matter micrograms/m3 size between 2.5 and 10 micrometers
    private double CO; //carbon monoxide ppm
    private long SO2;  //sulphur dioxide ppb
    private long NO2; //nitrogen dioxide ppb
    private double temp;
    private double humidity; // pourcent
    private double airpressure;  //hPa
    private double windspeed; // m/s
    private double winddir;
    private double batteryVoltage;
    private String gpsLocation; //gps coord
    private String location;
    private String label;
    private int unitId;

    public AirMonitorRecord(long o3,
                            double pm025,
                            double pm10,
                            double CO,
                            long SO2,
                            long NO2,
                            double temp,
                            double humidity,
                            double airpressure,
                            double windspeed,
                            double winddir,
                            double batteryVoltage,
                            String gpsLocation,
                            String location,
                            String label,
                            int unitId) {
        O3 = o3;
        this.pm025 = pm025;
        this.pm10 = pm10;
        this.CO = CO;
        this.SO2 = SO2;
        this.NO2 = NO2;
        this.temp = temp;
        this.humidity = humidity;
        this.airpressure = airpressure;
        this.windspeed = windspeed;
        this.winddir = winddir;
        this.batteryVoltage = batteryVoltage;
        this.gpsLocation = gpsLocation;
        this.location = location;
        this.label = label;
        this.unitId = unitId;
    }

    public long getO3() {
        return O3;
    }

    public void setO3(long o3) {
        O3 = o3;
    }

    public double getPm025() {
        return pm025;
    }

    public void setPm025(double pm025) {
        this.pm025 = pm025;
    }

    public double getPm10() {
        return pm10;
    }

    public void setPm10(double pm10) {
        this.pm10 = pm10;
    }

    public double getCO() {
        return CO;
    }

    public void setCO(double CO) {
        this.CO = CO;
    }

    public long getSO2() {
        return SO2;
    }

    public void setSO2(long SO2) {
        this.SO2 = SO2;
    }

    public long getNO2() {
        return NO2;
    }

    public void setNO2(long NO2) {
        this.NO2 = NO2;
    }

    public double getTemp() {
        return temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    public double getHumidity() {
        return humidity;
    }

    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }

    public double getAirpressure() {
        return airpressure;
    }

    public void setAirpressure(double airpressure) {
        this.airpressure = airpressure;
    }

    public double getWindspeed() {
        return windspeed;
    }

    public void setWindspeed(double windspeed) {
        this.windspeed = windspeed;
    }

    public double getWinddir() {
        return winddir;
    }

    public void setWinddir(double winddir) {
        this.winddir = winddir;
    }

    public double getBatteryVoltage() {
        return batteryVoltage;
    }

    public void setBatteryVoltage(double batteryVoltage) {
        this.batteryVoltage = batteryVoltage;
    }

    public String getGpsLocation() {
        return gpsLocation;
    }

    public void setGpsLocation(String gpsLocation) {
        this.gpsLocation = gpsLocation;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public int getUnitId() {
        return unitId;
    }

    public void setUnitId(int unitId) {
        this.unitId = unitId;
    }
}
