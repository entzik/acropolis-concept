package com.thekirschners.emplacements.column;

public interface EnvObservation {
    public long getTimestamp();
    public void setTimeStamp(long timeStamp);
    public double getTemperature();
    public void setTemperature(double temperature);
    public double getHumidity();
    public void setHumidity(double humidity);
    public double getWindSpeed();
    public void setWindSpeed(double windSpeed);
}
