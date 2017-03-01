package com.thekirschners.emplacements.column;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
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
    public Stream<EnvObservation> stream(int start, int end) {
        final Iterator<EnvObservation> iterator = iterator(start, end);
        final Spliterator<EnvObservation> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
        return StreamSupport.stream(spliterator, false);
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


    public class ArbitraryAccessCursor implements EnvObservation {
        private int crtNdx;

        protected ArbitraryAccessCursor(int ndx) {
            this.crtNdx = ndx;
        }

        public ArbitraryAccessCursor at(int ndx) {
            this.crtNdx = ndx;
            return this;
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
    }

    public class EnvObservationIterator  implements EnvObservation, Iterator<EnvObservation> {

        private int crtNdx;
        private final int end;

        protected EnvObservationIterator() {
            this.crtNdx = 1;
            this.end = EnvObsTimeSeries.this.position;
        }

        public EnvObservationIterator(int start, int end) {
            this.crtNdx = start - 1;
            this.end = end;
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

        @Override
        public boolean hasNext() {
            return this.crtNdx < end;
        }

        @Override
        public EnvObservationIterator next() {
            crtNdx ++;
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
}
