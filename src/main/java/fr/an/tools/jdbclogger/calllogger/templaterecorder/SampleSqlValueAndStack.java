package fr.an.tools.jdbclogger.calllogger.templaterecorder;

import fr.an.tools.jdbclogger.util.ExUtil;

/**
 * 
 */
public class SampleSqlValueAndStack {

    private String sqlWithValues;
    private String sqlStmt;
    private StackTraceElement[] stack;
    
    private int millis;
    
    // ------------------------------------------------------------------------

    public SampleSqlValueAndStack() {
    }

    // ------------------------------------------------------------------------

    public String getSqlWithValues() {
        return sqlWithValues;
    }

    public String getSqlStmt() {
        return sqlStmt;
    }

    public StackTraceElement[] getStack() {
        return stack;
    }
    
    public int getMillis() {
		return millis;
	}

	public void setInfos(StackTraceElement[] stack,
                         String firstSeenSqlWithValues,
                         String firstSeenSqlStmt,
                         int millis) {
        this.sqlWithValues = firstSeenSqlWithValues;
        this.sqlStmt = firstSeenSqlStmt;
        this.stack = stack;
        this.millis = millis;
    }
    
    public String toString() {
        return "(prepared)statement sql=" + sqlStmt + "\n"
               + "sql with inlined values=" + sqlWithValues + "\n"
               + "millis:" + millis + "\n"
               + ((stack != null) ? "stack=" + ExUtil.stackTraceToShortPath(stack) : "")
               + "\n";
    }

}
