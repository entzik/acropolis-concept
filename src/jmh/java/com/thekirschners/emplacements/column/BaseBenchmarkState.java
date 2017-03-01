package com.thekirschners.emplacements.column;

import java.util.Calendar;
import java.util.function.Consumer;

/**
 * Created by emilkirschner on 13/02/17.
 */
class BaseBenchmarkState {
    protected void generateTime(Consumer<Long> consumer) {
        for (int month = Calendar.JANUARY; month <= Calendar.DECEMBER; month++) {
            for (int day = 0; day <= 28; day++) {
                for (int hourOfDay = 0; hourOfDay < 24; hourOfDay++) {
                    for (int minute = 0; minute < 60; minute++) {
                        for (int seconds = 0; seconds < 60; seconds++) {
                            Calendar calendar = Calendar.getInstance();
                            calendar.set(2014, month, day, hourOfDay, minute, seconds);
                            calendar.set(Calendar.MILLISECOND, 0);
                            final long timeInMillis = calendar.getTimeInMillis();
                            consumer.accept(timeInMillis);
                        }
                    }
                }
            }
        }
    }

    protected long getEndTime(int year, int month) {
        final Calendar endCalendar = Calendar.getInstance();
        endCalendar.set(Calendar.YEAR, year);
        endCalendar.set(Calendar.MONTH, month);
        endCalendar.set(Calendar.DAY_OF_MONTH, endCalendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        endCalendar.set(Calendar.HOUR_OF_DAY, endCalendar.getMaximum(Calendar.HOUR_OF_DAY));
        endCalendar.set(Calendar.MINUTE, endCalendar.getMaximum(Calendar.HOUR_OF_DAY));
        endCalendar.set(Calendar.SECOND, endCalendar.getMaximum(Calendar.HOUR_OF_DAY));
        endCalendar.set(Calendar.MILLISECOND, endCalendar.getMaximum(Calendar.MILLISECOND));

        return endCalendar.getTimeInMillis();
    }

    protected long getStartTime(int year, int month) {
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
        return startCalendar.getTimeInMillis();
    }


}
