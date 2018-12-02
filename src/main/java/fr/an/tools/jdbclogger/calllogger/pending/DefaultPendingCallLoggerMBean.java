package fr.an.tools.jdbclogger.calllogger.pending;

/**
 * JMX MBean interface for controlling DefaultPendingCallLogger JMX beans
 */
public interface DefaultPendingCallLoggerMBean {

    public boolean isEnableLog();
    public void setEnableLog(boolean p);

    public boolean isEnableLogPre();
    public void setEnableLogPre(boolean p);
    
    public void logDumpPendingCall();
    public void logDumpPendingCall(long minWaitingMillis);
    
    public void dumpPendingActiveStatements();

    public void dumpPendingActiveStatements2(String fileName);

    public void dumpPendingActiveStatementsSince(long minWaitingMillis);

    public void dumpPendingActiveStatementsSince2(String fileName, long minWaitingMillis);

}
