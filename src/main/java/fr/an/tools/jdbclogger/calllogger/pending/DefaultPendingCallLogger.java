package fr.an.tools.jdbclogger.calllogger.pending;

import fr.an.tools.jdbclogger.calllogger.CallMsgInfo;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoFilter;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoFormat;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoLogger;
import fr.an.tools.jdbclogger.calllogger.DefaultCallMsgInfoFormat;
import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;
import fr.an.tools.jdbclogger.util.logadapter.AbstractLoggerAdapter;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * DefaultCallLogger
 *
 * logger for logging a call method
 * => logPre() / logPost() ... only 1 log writen, with Duration in millis seconds
 * 
 * possibility to log pending calls : see method logPendingCall()
 *  ... must be called from outside (for example : export logger in JMX... )
 *
 */
public class DefaultPendingCallLogger implements CallMsgInfoLogger, DefaultPendingCallLoggerMBean {

    private static CallMsgInfoFormat DEFAULT_FORMAT = DefaultCallMsgInfoFormat.fmtDefault;

    protected ConnectionFactoryConfig owner;

    protected boolean enableLog = true;

    /**
     * target log4j/slf4j logger
     */
    protected AbstractLoggerAdapter log;

    protected AbstractLoggerAdapter slowSqlLogger;

    protected boolean enableLogPre = false;

    /**
     * set<CallMsgInfo> 
     * pending calls = log.pre() entered, but call not finished
     */
    private final HashSet<CallMsgInfo> pending = new HashSet<>();

    /**
     * filter for message 
     */
    private CallMsgInfoFilter logCallFilter;

    /**
     * Formatter for messages = ndc format (optional) + message format
     * formatter can be different depending of context: pre/post/exception/pending 
     */
    //    private CallMsgInfoFormat logNdcFormat = null;
    private CallMsgInfoFormat logPreFormat = DEFAULT_FORMAT;
    private CallMsgInfoFormat logPostFormat = DEFAULT_FORMAT;
    private CallMsgInfoFormat logPostExFormat = DEFAULT_FORMAT;
    private CallMsgInfoFormat logPendingFormat = DEFAULT_FORMAT;

    /**
     * for JHMX: child name to export this object as MBean 
     */
    private String jmxChildNameToExport;

    protected String defaultPendingPrefixFileName = "jdbclog-pendings";

    // constructor
    // ------------------------------------------------------------------------

    /** full Ctor 
     * @param loggerProperties*/
    public DefaultPendingCallLogger(ConnectionFactoryConfig owner,
                                    String logName,
                                    CallMsgInfoFilter logCallFilter,
                                    Properties loggerProperties) {
        this.owner = owner;

        this.log = owner.getLogger(logName);
        this.enableLogPre = loggerProperties.getProperty("log_logPreMsg", "false").equalsIgnoreCase("true");
        this.logCallFilter = logCallFilter;

        this.slowSqlLogger = owner.getLogger("slow." + logName);
        
        // properties to configure MBean JMXExporter
        this.jmxChildNameToExport = "Type=PendingCallLogger,ConfigId=" + owner.getConfigId();
        owner.tryJmxRegisterChildMBean(jmxChildNameToExport, this);

    }

    /** internal */
    protected boolean isCurrentLogEnabled() {
        return enableLog && owner.isCurrentActiveLogs();
    }

    /** internal */
    protected boolean isSlowSqlLogsEnable() {
        return enableLog && owner.isSlowSqlLogsEnable();
    }
    
    public ConnectionFactoryConfig getOwner() {
        return owner;
    }
    
    public AbstractLoggerAdapter getLogger(String name) {
        return owner.getLogger(name);
    }
    
    // implements CallLogger
    // ------------------------------------------------------------------------

    /** implements CallLogger */
    @Override
    public void log(String msg) {
        if (isCurrentLogEnabled() && log.isInfoEnabled()) {
            log.info(msg);
        }
    }

    /**
     * implements CallLogger
     *  do not write 2 lines per Statement => wait until Post or Ex
     *  (save in tmp hashtable for forceLogAllStatementsPreNotFinished)
     */
    @Override
    public void logPre(CallMsgInfo call) {
        synchronized (pending) {
            pending.add(call);
        }
        // no log here.. see logPost / logPendingCall

        if (isCurrentLogEnabled() && enableLogPre && log.isInfoEnabled()) {
            boolean acceptLog = true;
            if (logCallFilter != null) {
                try {
                    acceptLog = logCallFilter.acceptMsg(call);
                } catch (Exception ex) {
                    acceptLog = true; // no rethrow!
                }
            }
            if (acceptLog && log.isInfoEnabled()) {
                String msg = doFormatCallInfo(logPreFormat, call);
                log.info(msg);
            } else {
                // ignore msg!
            }
        }

    }

    @Override
    public void preIgnoreLog(CallMsgInfo call) {
        synchronized (pending) {
            pending.add(call);
        }
    }

    /** implements CallLogger */
    @Override
    public void postIgnoreLog(CallMsgInfo call) {
        synchronized (pending) {
            pending.remove(call);
        }
    }

    /** implements CallLogger */
    @Override
    public void logPost(CallMsgInfo call) {
        synchronized (pending) {
            pending.remove(call);
        }
        int millis = call.getMillis();
        if (isCurrentLogEnabled() && log.isInfoEnabled()) {
            boolean acceptLog = true;
            if (logCallFilter != null) {
                try {
                    acceptLog = logCallFilter.acceptMsg(call);
                } catch (Exception ex) {
                    acceptLog = true; // no rethrow!
                }
            }
            if (millis > 2000) {
                @SuppressWarnings("unused")
                int dbg = 0;
                dbg++;
            }
            if (acceptLog && log.isInfoEnabled()) {
                String msg = doFormatCallInfo(logPostFormat, call);
                log.info(msg);
                @SuppressWarnings("unused")
                int dbg = 0;
                dbg++;
            } else {
                // ignore msg!
            }
        }
        if (millis > owner.getSlowSqlThresholdMillis()
        		&& isSlowSqlLogsEnable() 
            	&& slowSqlLogger.isInfoEnabled()) {
        	String msg = doFormatCallInfo(logPostFormat, call);
            slowSqlLogger.info(msg);
        }
    }

    /** implements CallLogger */
    @Override
    public void logPostEx(CallMsgInfo call) {
        synchronized (pending) {
            pending.remove(call);
        }
        if (isCurrentLogEnabled()) {
            String msg = doFormatCallInfo(logPostExFormat, call);
            log.error(msg); // , call.getException() already printed in format
        }
    }

    @Override
    public void logPostFetch(CallMsgInfo msgInfo) {
    	// do nothing?
    }
    	
    /**
     * implements CallLogger
     */
    @Override
    public void logPendingCall(long minWaitingMillis) {
        synchronized (pending) {
            if (log.isInfoEnabled()) {
                log.info(">>>>>> flush pending call: " + pending.size() + " elements " + "(filter timeEllapsed > "
                         + minWaitingMillis + ")");
                long timeNanos = System.nanoTime();
                for (Iterator<CallMsgInfo> ite = pending.iterator(); ite.hasNext();) {
                    CallMsgInfo elt = ite.next();
                    int elapsedMillis = (int) ((timeNanos - elt.getTimePreNanos())/1000000);
                    if (elapsedMillis > minWaitingMillis) {
                        String msg = doFormatCallInfo(logPendingFormat, elt);
                        log.info("PENDING " + " " + msg 
                                // + " started " // at:" + new Date(timePre) 
                                + " elapsed " + elapsedMillis + " ms");
                    } else {
                        // else too recent to be logged
                    }
                }
                log.info("<<<<< done flush pending call: " + pending.size() + " elements " + "(filter timeEllapsed > "
                         + minWaitingMillis + ")");

            }
        }
    }

    /** internal utility */
    private String doFormatCallInfo(CallMsgInfoFormat fmt, CallMsgInfo callInfo) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintWriter printer = new PrintWriter(bout);
        try {
            fmt.format(printer, callInfo);
        } catch (Exception ex) {
            printer.print("** FAILED TO FORMAT MESSAGE**");
        }
        printer.flush();
        String msg = bout.toString();
        return msg;
    }

    public AbstractLoggerAdapter getSlowSqlLogger() {
        return slowSqlLogger;
    }
    
    // implements DefaultPendingCallLoggerMBean
    // ------------------------------------------------------------------------

    /** implements DefaultPendingCallLoggerMBean */
    @Override
    public boolean isEnableLog() {
        return enableLog;
    }

    /** implements DefaultPendingCallLoggerMBean */
    @Override
    public void setEnableLog(boolean p) {
        this.enableLog = p;
    }

    /** implements DefaultPendingCallLoggerMBean */
    @Override
    public boolean isEnableLogPre() {
        return enableLogPre;
    }
    
    /** implements DefaultPendingCallLoggerMBean */
    @Override
    public void setEnableLogPre(boolean p) {
        this.enableLogPre = p;
    }
    
    /** implements DefaultPendingCallLoggerMBean */
    @Override
    public void logDumpPendingCall() {
        logPendingCall(0);
    }

    /** implements DefaultPendingCallLoggerMBean */
    @Override
    public void logDumpPendingCall(long minWaitingMillis) {
        logPendingCall(minWaitingMillis);
    }

    /** implements DefaultPendingCallLoggerMBean */
    @Override
    public void dumpPendingActiveStatements() {
        String fileName = defaultPendingPrefixFileName + ".txt";
        doDumpPendingActiveStatementsSince(fileName, 0);
    }

    /** implements DefaultPendingCallLoggerMBean */
    @Override
    public void dumpPendingActiveStatements2(String fileName) {
        doDumpPendingActiveStatementsSince(fileName, 0);
    }

    /** implements DefaultPendingCallLoggerMBean */
    @Override
    public void dumpPendingActiveStatementsSince(long minWaitingMillis) {
        String fileName = defaultPendingPrefixFileName + ".txt";
        doDumpPendingActiveStatementsSince(fileName, minWaitingMillis);
    }

    /** implements DefaultPendingCallLoggerMBean */
    @Override
    public void dumpPendingActiveStatementsSince2(String fileName, long minWaitingMillis) {
        doDumpPendingActiveStatementsSince(fileName, minWaitingMillis);
    }

    public void doDumpPendingActiveStatementsSince(String fileName, long minWaitingMillis) {
        log.info("write PendingStatements to file=" + fileName + ((minWaitingMillis > 0)? " since=" + minWaitingMillis : ""));
        File file = new File(fileName);
        PrintWriter out = null;
        try {
            out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(file)));
            doPrintPendingActiveStatements(out, minWaitingMillis);
        } catch (Exception ex) {
            log.error("failed to write file " + file + "'", ex);
            // ignore, no rethrow!
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception ex) {
                }
            }
        }
    }

    protected void doPrintPendingActiveStatements(PrintWriter out, long minWaitingMillis) {
        synchronized (pending) {
            long timeNanos = System.nanoTime();
            out.println("flush pending call: " + pending.size() + " elements " + "(filter timeEllapsed > "
                        + minWaitingMillis + ")");
            for (CallMsgInfo elt : pending) {
                int elapsedMillis = (int) ((timeNanos - elt.getTimePreNanos())/1000000);
                if (elapsedMillis > minWaitingMillis) {
                    String msg = doFormatCallInfo(logPendingFormat, elt);
                    out.println("PENDING " + " " + msg 
                            // + " started at:" + new Date(timePre) 
                            + " elapsed " + elapsedMillis + " ms");
                } // else too recent to be logged
            }
        }
    }

    public List<CallMsgInfo> getPendingCopy() {
        List<CallMsgInfo> res = new ArrayList<>();
        synchronized (pending) {
            res.addAll(pending);
        }
        return res;
    }
    
    public List<CallMsgInfo> getPendingCopySince(long minWaitingMillis) {
        List<CallMsgInfo> res = new ArrayList<>();
        synchronized (pending) {
            long timeNanos = System.nanoTime();
            for (CallMsgInfo elt : pending) {
                int elapsedMillis = (int) ((timeNanos - elt.getTimePreNanos())/1000000);
                if (elapsedMillis > minWaitingMillis) {
                    res.add(elt);
                }
            }
        }
        return res;
    }
    
    
}
