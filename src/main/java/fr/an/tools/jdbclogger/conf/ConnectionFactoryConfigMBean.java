package fr.an.tools.jdbclogger.conf;

/**
 * JMX interface to export DriverLog MBean 
 */
public interface ConnectionFactoryConfigMBean {

//	public String getName();
	public String getConnectionURL();
	
	// ------------------------------------------------------------------------
	
	
    public boolean isCurrentActiveLogs();

    public void setCurrentActiveLogs(boolean p);

    public boolean isCurrentActiveCommitLogs();

    public void setCurrentActiveCommitLogs(boolean p);

    public boolean isCurrentActiveRollbackLogs();
    public void setCurrentActiveRollbackLogs(boolean p);

    public boolean isCurrentActiveStatementCloseLogs();
    public void setCurrentActiveStatementCloseLogs(boolean p);

    public boolean isCurrentInterceptExceptionDuplicateKey();
    public void setCurrentInterceptExceptionDuplicateKey(boolean p);
    
    public boolean isSlowSqlLogsEnable();
	public void setSlowSqlLogsEnable(boolean p);

	public int getSlowSqlThresholdMillis();
	public void setSlowSqlThresholdMillis(int p);

	// Management of TemplateSqlEntryStatsCallLogger
    // ------------------------------------------------------------------------

    public void addStats(String name);

    public void removeStats(String name);

    public int getStatCount();

    public String getStatNames();

    /** helper for TemplateSqlEntryStatsCallLoggerMBean */
    public void clearStatSqlStats(String name);

    /** helper for TemplateSqlEntryStatsCallLoggerMBean */
    public void clearStatTemplateSqlEntries(String name);

    /** helper for TemplateSqlEntryStatsCallLoggerMBean */
    public void dumpCsvStatsFile(String name, String dumpFileName);

    /** helper for TemplateSqlEntryStatsCallLoggerMBean */
    public String getCsvDumpStats(String name);

    /** helper for TemplateSqlEntryStatsCallLoggerMBean */
    public String[][] getArrayDumpStats(String name);

}
