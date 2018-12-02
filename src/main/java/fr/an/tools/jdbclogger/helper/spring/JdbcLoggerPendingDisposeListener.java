package fr.an.tools.jdbclogger.helper.spring;

import fr.an.tools.jdbclogger.DriverLog;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfo;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoListLogger;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoLogger;
import fr.an.tools.jdbclogger.calllogger.pending.DefaultPendingCallLogger;
import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;
import fr.an.tools.jdbclogger.conf.DriverConfigLog;
import fr.an.tools.jdbclogger.jdbcimpl.StatementCallMsgInfo;
import fr.an.tools.jdbclogger.util.logadapter.AbstractLoggerAdapter;

import java.sql.Statement;
import java.util.List;
import java.util.Map;


public class JdbcLoggerPendingDisposeListener {

    public JdbcLoggerPendingDisposeListener() {
    }
    
    public void dispose() {
        DriverConfigLog conf = DriverLog.getDefaultInstance(); // Ugly singleton ...
        
        Map<String,ConnectionFactoryConfig> conFactoryMap = conf.getConnectionFactoryConfigMapCopy();
        for (ConnectionFactoryConfig conFactory : conFactoryMap.values()) {
            DefaultPendingCallLogger pendingCallLogger = conFactory.getPendingCallLogger();
            if (pendingCallLogger == null) {
                continue;
            }
            handleDisposePendingCallLogger(pendingCallLogger);
        }
        
//        CallMsgInfoListLogger callLoggerList = conf.getCallLoggerList();
//        List<CallMsgInfoLogger> appenders = callLoggerList.getAppenders();
//        for(CallMsgInfoLogger appender : appenders) {
//            if (appender instanceof DefaultPendingCallLogger) {
//                DefaultPendingCallLogger pendingCallLogger = (DefaultPendingCallLogger) appender;
//                handleDisposePendingCallLogger(pendingCallLogger);
//            }
//        }
    }

    protected void handleDisposePendingCallLogger(DefaultPendingCallLogger pendingCallLogger) {
        List<CallMsgInfo> pendingCallInfos = pendingCallLogger.getPendingCopy();
        if (pendingCallInfos != null && !pendingCallInfos.isEmpty()) {
            for(CallMsgInfo pendingCallInfo : pendingCallInfos) {
                handleDisposePendingCall(pendingCallLogger, pendingCallInfo);
            }
        }
    }
    
    protected void handleDisposePendingCall(DefaultPendingCallLogger pendingCallLogger, CallMsgInfo pendingCallInfo) {
        Statement stmt = null;
        if (pendingCallInfo instanceof StatementCallMsgInfo) {
            stmt = ((StatementCallMsgInfo) pendingCallInfo).getOwnerStatement();
        }
        AbstractLoggerAdapter logger = pendingCallLogger.getLogger("db");
        AbstractLoggerAdapter slowSqlLogger = pendingCallLogger.getSlowSqlLogger();
        
        if (stmt != null) {
            String msg = "*** detected still pending SQL query from jdbc-logger dispose => cancel it! " + pendingCallInfo.getMsg();
            slowSqlLogger.warn(msg);
            logger.warn(msg);
            try {
                stmt.cancel();
            } catch(Exception ex) {
                String errMsg = "*** Failed to cancel() ... ignore " + ex.getMessage();
                slowSqlLogger.warn(errMsg);
                logger.warn(errMsg);
                // ignore .. no rethrow!!
            }
        } else {
            String msg = "*** detected still pending SQL query from jdbc-logger dispose (but nothing to cancel?): " + pendingCallInfo.getMsg();
            slowSqlLogger.warn(msg);
            logger.warn(msg);
        }
    }

}
