package com.thekirschners.emplacements.column;

public interface EnvObservation {
    long getTimestamp();
    void setTimeStamp(long timeStamp);
    double getTemperature();
    void setTemperature(double temperature);
    double getHumidity();
    void setHumidity(double humidity);
    double getWindSpeed();
    void setWindSpeed(double windSpeed);
}
