package fr.an.tools.jdbclogger.util;

import java.util.Calendar;

/**
 * utility Pair for date/Time/Timestamp + calendar 
 */
public class ValueDateWithCal {
    Object date; // Date, Time, Timestamp
    Calendar cal;

    public ValueDateWithCal(Object date, Calendar cal) {
        this.date = date;
        this.cal = cal;
    }

    public String toString() {
        String res;
        if (date != null) {
            res = "'" + date + ((cal != null) ? (" " + cal) : "") + "'";
        } else {
            res = "null";
        }
        return res;
    }
}
