package com.thekirschners.emplacements.column;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class EnvObsTimeSeries implements Iterable<EnvObservation>{
    final long[] timestamps;
    final double[] temparatures;
    final double[] humidity;
    final double[] windSpeeds;

    final private int capacity;

    private int position;

    public EnvObsTimeSeries(int capacity) {
        this.capacity = capacity;

        this.timestamps = new long[capacity];
        this.temparatures = new double[capacity];
        this.humidity = new double[capacity];
        this.windSpeeds = new double[capacity];
        this.position = -1;
    }

    public void emplace(long timestamp, double temperature, double humidity, double windSpeed) {
        this.position++;
        this.timestamps[position] = timestamp;
        this.temparatures[position] = temperature;
        this.humidity[position] = humidity;
        this.windSpeeds[position] = windSpeed;
    }

    public void emplaceAt(int pos, long timestamp, double temperature, double humidity, double windSpeed) {
        this.timestamps[pos] = timestamp;
        this.temparatures[pos] = temperature;
        this.humidity[pos] = humidity;
        this.windSpeeds[pos] = windSpeed;
    }

    public int[] extractTimeSlice(long minTimestamp, long maxTimestamp) {
        int[] ret = new int[]{
                findLowerTimestampBound(minTimestamp),
                findUpperTimestampBound(maxTimestamp)
        };
        return ret;
    }

    public LongStream getTimestamps() {
        return Arrays.stream(timestamps);
    }

    public DoubleStream getTemperatures() {
        return Arrays.stream(temparatures);
    }

    public DoubleStream getTemperatures(int min, int max) {
        return Arrays.stream(temparatures, min, max);
    }

    public DoubleStream getHumidity() {
        return Arrays.stream(humidity);
    }

    public DoubleStream getWindSpeeds() {
        return Arrays.stream(windSpeeds);
    }

    public EnvObservationIterator iterator() {
        return new EnvObservationIterator();
    }

    public Iterator<EnvObservation> iterator(int start, int end) {
        return new EnvObservationIterator(start, end);
    }


    public Stream<EnvObservation> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    public ArbitraryAccessCursor get(int ndx) {
        return new ArbitraryAccessCursor(ndx);
    }

    private int findUpperTimestampBound(long max) {
        int maxNdx = Arrays.binarySearch(timestamps, max);
        if (maxNdx < 0)
            maxNdx = (-maxNdx) - 1;
        while (maxNdx < (timestamps.length - 1) && timestamps[maxNdx] == timestamps[maxNdx + 1])
            maxNdx++;
        return maxNdx;
    }

    private int findLowerTimestampBound(long min) {
        int minNdx = Arrays.binarySearch(timestamps, min);
        if (minNdx < 0)
            minNdx = (-minNdx) - 1;
        while (minNdx > 0 && timestamps[minNdx] == timestamps[minNdx - 1])
            minNdx--;
        return minNdx;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getSize() {
        return position + 1;
    }


    public class ArbitraryAccessCursor extends EnvObservationItem {
        protected ArbitraryAccessCursor(int ndx) {
            super(ndx);
        }

        public ArbitraryAccessCursor at(int ndx) {
            setCrtNdx(ndx);
            return this;
        }
    }

    public class EnvObservationIterator extends EnvObservationItem implements Iterator<EnvObservation> {

        private final int end;

        protected EnvObservationIterator() {
            super(-1);
            this.end = EnvObsTimeSeries.this.position;
        }

        public EnvObservationIterator(int start, int end) {
            super(start - 1);
            this.end = end;
        }

        @Override
        public boolean hasNext() {
            return this.getCrtNdx() < EnvObsTimeSeries.this.position;
        }

        @Override
        public EnvObservationIterator next() {
            incNdx();
            return this;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forEachRemaining(Consumer<? super EnvObservation> action) {
            while (hasNext())
                action.accept(next());
        }
    }

    protected class EnvObservationItem implements EnvObservation {
        private int crtNdx;

        public int getCrtNdx() {
            return crtNdx;
        }

        protected EnvObservationItem setCrtNdx(int crtNdx) {
            this.crtNdx = crtNdx;
            return this;
        }

        protected void incNdx() {
            this.crtNdx ++;
        }


        @Override
        public long getTimestamp() {
            return EnvObsTimeSeries.this.timestamps[crtNdx];
        }

        @Override
        public void setTimeStamp(long timeStamp) {
            EnvObsTimeSeries.this.timestamps[crtNdx] = timeStamp;
        }

        @Override
        public double getTemperature() {
            return EnvObsTimeSeries.this.temparatures[crtNdx];
        }

        @Override
        public void setTemperature(double temperature) {
            EnvObsTimeSeries.this.temparatures[crtNdx] = temperature;
        }

        @Override
        public double getHumidity() {
            return EnvObsTimeSeries.this.humidity[crtNdx];
        }

        @Override
        public void setHumidity(double humidity) {
            EnvObsTimeSeries.this.humidity[crtNdx] = humidity;
        }

        @Override
        public double getWindSpeed() {
            return EnvObsTimeSeries.this.windSpeeds[crtNdx];
        }

        @Override
        public void setWindSpeed(double windSpeed) {
            EnvObsTimeSeries.this.windSpeeds[crtNdx] = windSpeed;
        }

        protected EnvObservationItem(int ndx) {
            this.crtNdx = ndx;
        }

    }
}
