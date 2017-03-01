package com.thekirschners.emplacements.column;

/**
 * Created by emilkirschner on 08/02/17.
 */
public class EnvObservationImpl implements EnvObservation {
    private long timestamp;
    private double temperature;
    private double humidity;
    private double windSpeed;

    public EnvObservationImpl(long timestamp, double temperature, double humidity, double windSpeed) {
        this.timestamp = timestamp;
        this.temperature = temperature;
        this.humidity = humidity;
        this.windSpeed = windSpeed;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimeStamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public double getTemperature() {
        return temperature;
    }

    @Override
    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    @Override
    public double getHumidity() {
        return humidity;
    }

    @Override
    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }

    @Override
    public double getWindSpeed() {
        return windSpeed;
    }

    @Override
    public void setWindSpeed(double windSpeed) {
        this.windSpeed = windSpeed;
    }
}
