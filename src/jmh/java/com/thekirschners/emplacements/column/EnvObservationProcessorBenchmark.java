package com.thekirschners.emplacements.column;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class EnvObservationProcessorBenchmark {

    public static final int CAPACITY = 12 * 30 * 24 * 60 * 60 + 1;

    @State(Scope.Thread)
    public static class BenchmarkState extends BaseBenchmarkState{
        final EnvObsTimeSeries envObservationColumnarStore = new EnvObsTimeSeries(CAPACITY);

        @Setup(Level.Trial)
        public void doSetup() {
            generateTime(timeInMillis -> envObservationColumnarStore.emplace(timeInMillis, Math.random(), Math.random(), Math.random()));
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
        }

        public EnvObsTimeSeries getEnvObservationColumnarStore() {
            return envObservationColumnarStore;
        }
    }

    @State(Scope.Thread)
    public static class ClassicBenchmarkState extends BaseBenchmarkState{
        final ArrayList<EnvObservation> envObservationColumnarList = new ArrayList<>();

        @Setup(Level.Trial)
        public void doSetup() {
            generateTime(timeInMillis -> envObservationColumnarList.add(new EnvObservationImpl(timeInMillis, Math.random(), Math.random(), Math.random())));
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
        }

        public List<EnvObservation> getEnvObservationColumnarList(int year, int month) {
            final long startTime = getStartTime(year, month);
            final long endTime = getEndTime(year, month);

            final int[] ints = extractTimeSlice(startTime, endTime);
            return envObservationColumnarList.subList(ints[0], ints[1]);
        }

        private int[] extractTimeSlice(long startTime, long endTime) {
            int start = Collections.binarySearch(envObservationColumnarList, new EnvObservationImpl(startTime, 0, 0, 0), (o1, o2) -> (int) (o1.getTimestamp() - o2.getTimestamp()));
            if (start < 0)
                start = (-start) - 1;
            int end = Collections.binarySearch(envObservationColumnarList, new EnvObservationImpl(endTime, 0, 0, 0), (o1, o2) -> (int) (o1.getTimestamp() - o2.getTimestamp()));
            if (end < 0)
                end = (-end) - 1;
            return new int[]{start, end};
        }
    }

    @State(Scope.Thread)
    public static class ClassicBenchmarkStateShuffled extends BaseBenchmarkState {
        final ArrayList<EnvObservation> envObservationColumnarList = new ArrayList<>();

        @Setup(Level.Trial)
        public void doSetup() {
            generateTime(timeInMillis -> envObservationColumnarList.add(new EnvObservationImpl(timeInMillis, Math.random(), Math.random(), Math.random())));

            // shuffle
            for (int i = 0; i < envObservationColumnarList.size(); i +=4) {
                int p1 = (int)(Math.random() * envObservationColumnarList.size());
                int p2 = (int)(Math.random() * envObservationColumnarList.size());
                EnvObservation p = envObservationColumnarList.get(p1);
                envObservationColumnarList.set(p1, envObservationColumnarList.get(p2));
                envObservationColumnarList.set(p2, p);
            }

            // reset timestamps
            AtomicInteger crt = new AtomicInteger(0);
            generateTime(timeStamp -> envObservationColumnarList.get(crt.getAndIncrement()).setTimeStamp(timeStamp));
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
        }

        public List<EnvObservation> getEnvObservationColumnarList(int year, int month) {
            final long startTime = getStartTime(year, month);
            final long endTime = getEndTime(year, month);

            final int[] ints = extractTimeSlice(startTime, endTime);
            return envObservationColumnarList.subList(ints[0], ints[1]);
        }

        private int[] extractTimeSlice(long startTime, long endTime) {
            int start = Collections.binarySearch(envObservationColumnarList, new EnvObservationImpl(startTime, 0, 0, 0), (o1, o2) -> (int) (o1.getTimestamp() - o2.getTimestamp()));
            if (start < 0)
                start = (-start) - 1;
            int end = Collections.binarySearch(envObservationColumnarList, new EnvObservationImpl(endTime, 0, 0, 0), (o1, o2) -> (int) (o1.getTimestamp() - o2.getTimestamp()));
            if (end < 0)
                end = (-end) - 1;
            return new int[]{start, end};
        }

    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void streamBenchmark(BenchmarkState state, Blackhole bh) {
        final OptionalDouble optionalDouble = EnvObsevationsProcessor.averageMonthlyTemperatureWithPrimitiveStream(state.getEnvObservationColumnarStore(), 2014, Calendar.MARCH);
        bh.consume(optionalDouble);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void cursorBenchmark(BenchmarkState state, Blackhole bh) {
        final OptionalDouble optionalDouble = EnvObsevationsProcessor.averageMonthlyTemperatureWithCursor(state.getEnvObservationColumnarStore(), 2014, Calendar.MARCH);
        bh.consume(optionalDouble);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void iteratorBenchmark(BenchmarkState state, Blackhole bh) {
        final OptionalDouble optionalDouble = EnvObsevationsProcessor.averageMonthlyTemperatureWithObjectItertor(state.getEnvObservationColumnarStore(), 2014, Calendar.MARCH);
        bh.consume(optionalDouble);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void objectStreamBenchmark(BenchmarkState state, Blackhole bh) {
        final OptionalDouble optionalDouble = EnvObsevationsProcessor.averageMonthlyTemperatureWithObjectStream(state.getEnvObservationColumnarStore(), 2014, Calendar.MARCH);
        bh.consume(optionalDouble);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void classicBenchmark(ClassicBenchmarkState state, Blackhole bh) {
        final OptionalDouble optionalDouble = state.getEnvObservationColumnarList(2014, Calendar.MARCH).stream().mapToDouble(EnvObservation::getTemperature).average();
        bh.consume(optionalDouble);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void classicBenchmarkShuffled(ClassicBenchmarkStateShuffled state, Blackhole bh) {
        final OptionalDouble optionalDouble = state.getEnvObservationColumnarList(2014, Calendar.MARCH).stream().mapToDouble(EnvObservation::getTemperature).average();
        bh.consume(optionalDouble);
    }
}
