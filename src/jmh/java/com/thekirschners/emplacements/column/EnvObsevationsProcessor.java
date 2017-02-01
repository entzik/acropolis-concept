package com.thekirschners.emplacements.column;

import java.util.Calendar;
import java.util.Iterator;
import java.util.OptionalDouble;

public class EnvObsevationsProcessor {
    public static OptionalDouble averageMonthlyTemperatureWithStream(EnvObsTimeSeries columnarStore, int year, int month) {
        final Calendar startCalendar = Calendar.getInstance();
        startCalendar.set(
                year,
                month,
                startCalendar.getMinimum(Calendar.DAY_OF_MONTH),
                startCalendar.getMinimum(Calendar.HOUR_OF_DAY),
                startCalendar.getMinimum(Calendar.MINUTE),
                startCalendar.getMinimum(Calendar.SECOND)
        );
        startCalendar.set(Calendar.MILLISECOND, startCalendar.getMinimum(Calendar.MILLISECOND));
        final long startTime = startCalendar.getTimeInMillis();

        final Calendar endCalendar = Calendar.getInstance();
        endCalendar.set(Calendar.YEAR, year);
        endCalendar.set(Calendar.MONTH, month);
        endCalendar.set(Calendar.DAY_OF_MONTH, endCalendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        endCalendar.set(Calendar.HOUR_OF_DAY, endCalendar.getMaximum(Calendar.HOUR_OF_DAY));
        endCalendar.set(Calendar.MINUTE, endCalendar.getMaximum(Calendar.HOUR_OF_DAY));
        endCalendar.set(Calendar.SECOND, endCalendar.getMaximum(Calendar.HOUR_OF_DAY));
        endCalendar.set(Calendar.MILLISECOND, endCalendar.getMaximum(Calendar.MILLISECOND));

        final long endTime = endCalendar.getTimeInMillis();

        final int[] timeSliceIndexes = columnarStore.extractTimeSlice(startTime, endTime);

        return columnarStore.getTemperatures(timeSliceIndexes[0], timeSliceIndexes[1]).average();
    }

    public static OptionalDouble averageMonthlyTemperatureWithCursor(EnvObsTimeSeries columnarStore, int year, int month) {
        final Calendar startCalendar = Calendar.getInstance();
        startCalendar.set(
                year,
                month,
                startCalendar.getMinimum(Calendar.DAY_OF_MONTH),
                startCalendar.getMinimum(Calendar.HOUR_OF_DAY),
                startCalendar.getMinimum(Calendar.MINUTE),
                startCalendar.getMinimum(Calendar.SECOND)
        );
        startCalendar.set(Calendar.MILLISECOND, startCalendar.getMinimum(Calendar.MILLISECOND));
        final long startTime = startCalendar.getTimeInMillis();

        final Calendar endCalendar = Calendar.getInstance();
        endCalendar.set(Calendar.YEAR, year);
        endCalendar.set(Calendar.MONTH, month);
        endCalendar.set(Calendar.DAY_OF_MONTH, endCalendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        endCalendar.set(Calendar.HOUR_OF_DAY, endCalendar.getMaximum(Calendar.HOUR_OF_DAY));
        endCalendar.set(Calendar.MINUTE, endCalendar.getMaximum(Calendar.HOUR_OF_DAY));
        endCalendar.set(Calendar.SECOND, endCalendar.getMaximum(Calendar.HOUR_OF_DAY));
        endCalendar.set(Calendar.MILLISECOND, endCalendar.getMaximum(Calendar.MILLISECOND));

        final long endTime = endCalendar.getTimeInMillis();

        final int[] timeSliceIndexes = columnarStore.extractTimeSlice(startTime, endTime);

        double average = 0;
        double count = timeSliceIndexes[1] - timeSliceIndexes[0];
        final EnvObsTimeSeries.ArbitraryAccessCursor cursor = columnarStore.get(0);
        final int start = timeSliceIndexes[0];
        final int end = timeSliceIndexes[1];
        for (int i = start; i <= end; i ++)
            average += (cursor.at(i).getTemperature() / count);

        return OptionalDouble.of(average);
    }

    public static OptionalDouble averageMonthlyTemperatureWithItertor(EnvObsTimeSeries columnarStore, int year, int month) {
        final Calendar startCalendar = Calendar.getInstance();
        startCalendar.set(
                year,
                month,
                startCalendar.getMinimum(Calendar.DAY_OF_MONTH),
                startCalendar.getMinimum(Calendar.HOUR_OF_DAY),
                startCalendar.getMinimum(Calendar.MINUTE),
                startCalendar.getMinimum(Calendar.SECOND)
        );
        startCalendar.set(Calendar.MILLISECOND, startCalendar.getMinimum(Calendar.MILLISECOND));
        final long startTime = startCalendar.getTimeInMillis();

        final Calendar endCalendar = Calendar.getInstance();
        endCalendar.set(Calendar.YEAR, year);
        endCalendar.set(Calendar.MONTH, month);
        endCalendar.set(Calendar.DAY_OF_MONTH, endCalendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        endCalendar.set(Calendar.HOUR_OF_DAY, endCalendar.getMaximum(Calendar.HOUR_OF_DAY));
        endCalendar.set(Calendar.MINUTE, endCalendar.getMaximum(Calendar.HOUR_OF_DAY));
        endCalendar.set(Calendar.SECOND, endCalendar.getMaximum(Calendar.HOUR_OF_DAY));
        endCalendar.set(Calendar.MILLISECOND, endCalendar.getMaximum(Calendar.MILLISECOND));

        final long endTime = endCalendar.getTimeInMillis();

        final int[] timeSliceIndexes = columnarStore.extractTimeSlice(startTime, endTime);

        double average = 0;
        double count = timeSliceIndexes[1] - timeSliceIndexes[0];
        final int start = timeSliceIndexes[0];
        final int end = timeSliceIndexes[1];
        for (Iterator<EnvObservation> it = columnarStore.iterator(start, end); it.hasNext(); /* NOP */)
            average += (it.next().getTemperature() / count);

        return OptionalDouble.of(average);
    }
}
