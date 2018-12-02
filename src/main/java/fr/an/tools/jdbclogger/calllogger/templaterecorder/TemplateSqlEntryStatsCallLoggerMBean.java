package fr.an.tools.jdbclogger.calllogger.templaterecorder;

/**
 * interface for exporting DefaultStackTypeCallLogger as MBean in JMX
 */
public interface TemplateSqlEntryStatsCallLoggerMBean {

    // current runtime status 
    // ------------------------------------------------------------------------

    public boolean isCurrentActiveTemplateSqlEntryLogs();

    public boolean isCurrentActiveSqlEntryLogs();

    // configuration
    // ------------------------------------------------------------------------

    public boolean isActiveTemplateSqlEntryLogs();

    public void setActiveTemplateSqlEntryLogs(boolean p);

    public boolean isActiveSqlEntryLogs();

    public void setActiveSqlEntryLogs(boolean p);

    // stats, dump
    // ------------------------------------------------------------------------

    public int getTemplateSqlEntriesCount();

    public int getSqlEntriesCount();

    public void clearTemplateSqlEntries();

    public void loadTemplateSqlEntriesFromFileName(String fileName);

    /** clear stats, but do not remove template entries */
    public void clearSqlStats();

    // public String getDumpTemplateSqlEntries(boolean detail);
    public void writeFileDumpTemplateSqlEntries();

    public String getCsvDumpSqlStats();

    public String[][] getArrayDumpSqlStats();

    /**
     * print file for reading stats in Excel (comma separated value) for queries, sorted by descendant totalMillis
     */
    public void writeCsvFileDumpSqlStats();

    /**
     * print file for reading stats in Excel (comma separated value) for queries, sorted by descendant totalMillis
     */
    public void writeCsvFileDumpSqlStats2(String fileName);

    /**
     * write file for problematic PreparedStatement without Bind Variables (same sql template, but with several sql entries)
     */
    public void writeFileDumpProblemBindVariables();

}
