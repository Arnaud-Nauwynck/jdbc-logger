package fr.an.tools.jdbclogger.calllogger.templaterecorder;

import java.io.PrintWriter;

/**
 * Entry for keeping statistics on a given Sql (Prepared)Statment type 
 */
public class SqlEntry {

    private TemplateSqlEntry parentEntry;

    private final String sql;

    private final Stats stats = new Stats();

    private final SampleSqlValueAndStack firstSeenInfo = new SampleSqlValueAndStack();
    
    private final SampleSqlValueAndStack maxSeenInfo = new SampleSqlValueAndStack();

    
    // -------------------------------------------------------------------------

    public SqlEntry(TemplateSqlEntry parentEntry, String sql) {
        this.parentEntry = parentEntry;
        this.sql = sql;
    }

    // -------------------------------------------------------------------------

    public TemplateSqlEntry getParentEntry() {
        return parentEntry;
    }

    public String getSql() {
        return sql;
    }

    public Stats getStats() {
        return stats;
    }

    public SampleSqlValueAndStack getFirstSeenInfo() {
        return firstSeenInfo;
    }

    public SampleSqlValueAndStack getMaxSeenInfo() {
        return maxSeenInfo;
    }

    public void writeDump(PrintWriter out) {
        out.println("SqlEntry sql=" + sql + "\n" + "stats:" + stats + "\n" + firstSeenInfo);
    }

}
