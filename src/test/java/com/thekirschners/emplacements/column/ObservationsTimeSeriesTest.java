package com.thekirschners.emplacements.column;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static junit.framework.TestCase.assertEquals;

@RunWith(JUnit4.class)
public class ObservationsTimeSeriesTest {
    /**
     * verify the time series is properly initialized and the requested capacity is reserved
     */
    @Test
    public void capacityTest() {
        final int planedCapacity = 2435;

        EnvObsTimeSeries ts = new EnvObsTimeSeries(planedCapacity);

        assertEquals(planedCapacity, ts.getCapacity());
        assertEquals(planedCapacity, ts.timestamps.length);
        assertEquals(planedCapacity, ts.temparatures.length);
        assertEquals(planedCapacity, ts.windSpeeds.length);
        assertEquals(planedCapacity, ts.humidity.length);
        assertEquals(0, ts.getSize());
    }

    /**
     * verify that appended elements fill up the underlying columns (arrays) as expected
     */
    @Test
    public void emplacementTest() {
        final int planedCapacity = 32;

        final long timestamp = System.currentTimeMillis();
        EnvObsTimeSeries ts = buildEnvObsTimeSeries(planedCapacity, timestamp);

        assertEquals(5, ts.getSize());

        assertEquals(timestamp + 0, ts.timestamps[0]);
        assertEquals(timestamp + 1, ts.timestamps[1]);
        assertEquals(timestamp + 2, ts.timestamps[2]);
        assertEquals(timestamp + 3, ts.timestamps[3]);
        assertEquals(timestamp + 4, ts.timestamps[4]);

        assertEquals(1.0, ts.temparatures[0]);
        assertEquals(1.1, ts.temparatures[1]);
        assertEquals(1.2, ts.temparatures[2]);
        assertEquals(1.3, ts.temparatures[3]);
        assertEquals(1.4, ts.temparatures[4]);

        assertEquals(2.0, ts.humidity[0]);
        assertEquals(2.1, ts.humidity[1]);
        assertEquals(2.2, ts.humidity[2]);
        assertEquals(2.3, ts.humidity[3]);
        assertEquals(2.4, ts.humidity[4]);

        assertEquals(3.0, ts.windSpeeds[0]);
        assertEquals(3.1, ts.windSpeeds[1]);
        assertEquals(3.2, ts.windSpeeds[2]);
        assertEquals(3.3, ts.windSpeeds[3]);
        assertEquals(3.4, ts.windSpeeds[4]);
    }

    /**
     * verify that setting an element in the middle of the time series changes the target element and does not affect
     * any other elements
     */
    @Test
    public void emplacementAtTest() {
        final int planedCapacity = 32;
        final long timestamp = System.currentTimeMillis();
        EnvObsTimeSeries ts = buildEnvObsTimeSeries(planedCapacity, timestamp);

        ts.emplaceAt(1, timestamp + 6, 7.7, 8.8, 9.9);

        assertEquals(5, ts.getSize());

        assertEquals(timestamp + 0, ts.timestamps[0]);
        assertEquals(timestamp + 6, ts.timestamps[1]);
        assertEquals(timestamp + 2, ts.timestamps[2]);
        assertEquals(timestamp + 3, ts.timestamps[3]);
        assertEquals(timestamp + 4, ts.timestamps[4]);

        assertEquals(1.0, ts.temparatures[0]);
        assertEquals(7.7, ts.temparatures[1]);
        assertEquals(1.2, ts.temparatures[2]);
        assertEquals(1.3, ts.temparatures[3]);
        assertEquals(1.4, ts.temparatures[4]);

        assertEquals(2.0, ts.humidity[0]);
        assertEquals(8.8, ts.humidity[1]);
        assertEquals(2.2, ts.humidity[2]);
        assertEquals(2.3, ts.humidity[3]);
        assertEquals(2.4, ts.humidity[4]);

        assertEquals(3.0, ts.windSpeeds[0]);
        assertEquals(9.9, ts.windSpeeds[1]);
        assertEquals(3.2, ts.windSpeeds[2]);
        assertEquals(3.3, ts.windSpeeds[3]);
        assertEquals(3.4, ts.windSpeeds[4]);
    }

    /**
     * verify the cursor read values as expected
     */
    @Test
    public void cursorReadTest() {
        final int planedCapacity = 32;
        final long timestamp = System.currentTimeMillis();
        EnvObsTimeSeries ts = buildEnvObsTimeSeries(planedCapacity, timestamp);
        final EnvObsTimeSeries.ArbitraryAccessCursor cursor = ts.get(0);

        assertEquals(1.0, cursor.at(0).getTemperature());
        assertEquals(1.1, cursor.at(1).getTemperature());
        assertEquals(1.2, cursor.at(2).getTemperature());
        assertEquals(1.3, cursor.at(3).getTemperature());
        assertEquals(1.4, cursor.at(4).getTemperature());

        assertEquals(2.0, cursor.at(0).getHumidity());
        assertEquals(2.1, cursor.at(1).getHumidity());
        assertEquals(2.2, cursor.at(2).getHumidity());
        assertEquals(2.3, cursor.at(3).getHumidity());
        assertEquals(2.4, cursor.at(4).getHumidity());

        assertEquals(3.0, cursor.at(0).getWindSpeed());
        assertEquals(3.1, cursor.at(1).getWindSpeed());
        assertEquals(3.2, cursor.at(2).getWindSpeed());
        assertEquals(3.3, cursor.at(3).getWindSpeed());
        assertEquals(3.4, cursor.at(4).getWindSpeed());
    }

    /**
     * verify the cursor writes values as expected
     */
    @Test
    public void cursorWriteTest() {
        final int planedCapacity = 32;
        final long timestamp = System.currentTimeMillis();
        EnvObsTimeSeries ts = buildEnvObsTimeSeries(planedCapacity, timestamp);
        final EnvObsTimeSeries.ArbitraryAccessCursor cursor = ts.get(0);

        cursor.at(0).setTemperature(74.1);
        cursor.at(0).setHumidity(75.2);
        cursor.at(0).setWindSpeed(76.3);

        assertEquals(74.1, cursor.at(0).getTemperature());
        assertEquals(75.2, cursor.at(0).getHumidity());
        assertEquals(76.3, cursor.at(0).getWindSpeed());

    }

    @Test
    public void testExtractTimeSlice() {
        EnvObsTimeSeries ts = new EnvObsTimeSeries(1000);
        for (int i = 0; i < 1000; i ++)
            ts.emplace(i * 2, Math.random(), Math.random(), Math.random());

        final int[] ints = ts.extractTimeSlice(501, 521);
        assertEquals(251, ints[0]);
        assertEquals(261, ints[1]);
    }

    private EnvObsTimeSeries buildEnvObsTimeSeries(int planedCapacity, long timestamp) {
        EnvObsTimeSeries ts = new EnvObsTimeSeries(planedCapacity);
        ts.emplace(timestamp + 0, 1.0, 2.0, 3.0);
        ts.emplace(timestamp + 1, 1.1, 2.1, 3.1);
        ts.emplace(timestamp + 2, 1.2, 2.2, 3.2);
        ts.emplace(timestamp + 3, 1.3, 2.3, 3.3);
        ts.emplace(timestamp + 4, 1.4, 2.4, 3.4);
        return ts;
    }
}
