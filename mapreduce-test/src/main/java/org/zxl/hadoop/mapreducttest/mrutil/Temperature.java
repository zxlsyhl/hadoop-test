package org.zxl.hadoop.mapreducttest.mrutil;

public class Temperature {
    private int year;
    private String city;
    private int size;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Temperature(int year, String city, int size) {
        this.year = year;
        this.city = city;
        this.size = size;
    }
}
