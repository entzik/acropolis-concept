package com.thekirschners.emplacements.column;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Calendar;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;

public class EnvObservationProcessorBenchmark {

    public static final int CAPACITY = 3 * 12 * 31 * 24 * 60;

    @State(Scope.Thread)
    public static class BenchmarkState {
        final EnvObsTimeSeries envObservationColumnarStore = new EnvObsTimeSeries(CAPACITY);

        @Setup(Level.Trial)
        public void doSetup() {
            System.out.println("Do Setup");

            Calendar calendar = Calendar.getInstance();
            for (int year = 2014; year < 2017; year++) {
                calendar.set(Calendar.YEAR, year);
                for (int month = Calendar.JANUARY; month <= Calendar.DECEMBER; month++) {
                    calendar.set(Calendar.MONTH, month);
                    final int daysInMonth = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
                    for (int day = 0; day <= daysInMonth; day++) {
                        calendar.set(Calendar.DAY_OF_MONTH, day);
                        for (int hourOfDay = 0; hourOfDay < 24; hourOfDay++) {
                            calendar.set(Calendar.HOUR_OF_DAY, hourOfDay);
                            for (int minute = 0; minute < 60; minute++) {
                                calendar.set(Calendar.MINUTE, minute);
                                envObservationColumnarStore.emplace(calendar.getTimeInMillis(), Math.random(), Math.random(), Math.random());
                            }
                        }
                    }
                }
            }
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
            System.out.println("Do TearDown");
        }

        public EnvObsTimeSeries getEnvObservationColumnarStore() {
            return envObservationColumnarStore;
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void streamBenchmark(BenchmarkState state, Blackhole bh) {
        final OptionalDouble optionalDouble = EnvObsevationsProcessor.averageMonthlyTemperatureWithStream(state.getEnvObservationColumnarStore(), 2014, Calendar.MARCH);
        bh.consume(optionalDouble);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void cursorBenchmark(BenchmarkState state, Blackhole bh) {
        final OptionalDouble optionalDouble = EnvObsevationsProcessor.averageMonthlyTemperatureWithCursor(state.getEnvObservationColumnarStore(), 2014, Calendar.MARCH);
        bh.consume(optionalDouble);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void iteratorBenchmark(BenchmarkState state, Blackhole bh) {
        final OptionalDouble optionalDouble = EnvObsevationsProcessor.averageMonthlyTemperatureWithItertor(state.getEnvObservationColumnarStore(), 2014, Calendar.MARCH);
        bh.consume(optionalDouble);
    }
}
