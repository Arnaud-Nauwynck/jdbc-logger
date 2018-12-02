package fr.an.tools.jdbclogger.conf;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.sql.XAConnection;

import fr.an.tools.jdbclogger.calllogger.CallMsgInfoFilter;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoListLogger;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoLogger;
import fr.an.tools.jdbclogger.calllogger.DefaultCallMsgInfoFilter;
import fr.an.tools.jdbclogger.calllogger.pending.DefaultPendingCallLogger;
import fr.an.tools.jdbclogger.calllogger.templaterecorder.TemplateSqlEntryStatsCallLogger;
import fr.an.tools.jdbclogger.jdbcimpl.JdbcLoggerConnection;
import fr.an.tools.jdbclogger.jdbcimpl.JdbcLoggerXAConnection;
import fr.an.tools.jdbclogger.util.FileUtil;
import fr.an.tools.jdbclogger.util.logadapter.AbstractLoggerAdapter;

/**
 * internal configuration for a given URL of connections factory
 */
public class ConnectionFactoryConfig implements ConnectionFactoryConfigMBean {

    public static final String RESOURCENAME_DISCARD = "jdbclogger-db-discard.txt";
    public static final String RESOURCENAME_DISCARDPREFIX = "jdbclogger-db-discard-prefix.txt";

    public static final String LOG_PROPERTY_PREFIX = "log_";

    private static final String UNDERLYING_DRIVER_CLASS = LOG_PROPERTY_PREFIX + "driverclass";

    
    // ------------------------------------------------------------------------
    
    private DriverConfigLog ownerDriver;
    private int configId;
    private String name;
    private String connectionURL;
    
    /** default properties for logging per connection */
    protected Properties properties;

    protected AbstractLoggerAdapter log;
    
    protected DriverHelper driverHelper;
    
    protected boolean useTemplateSqlEntryStatsCallLogger = true;

    /**
     * composite list of CallLogger added at runtime for this URL pool
     */
    protected CallMsgInfoListLogger callLoggerList = new CallMsgInfoListLogger();

    
    /**
     * the logger for this URL pool
     */
    protected CallMsgInfoLogger defaultCallMsgInfoLogger;

    /**
     * the multi-threaded TemplateRecorder CallLogger for this URL pool
     */
    protected TemplateSqlEntryStatsCallLogger templateSqlEntryStatsCallLogger;

    
    boolean logOpenClose = true;

    /**
     * flags to globally enable/disable all log4j logs 
     */
    protected boolean currentActiveLogs = true;

    /**
     * flags to globally enable/disable "commit / setAutoCommit / setIsolationMode" log4j logs 
     */
    protected boolean currentActiveCommitLogs = false;

    /**
     * flags to globally enable/disable "rollback" log4j logs 
     */
    protected boolean currentActiveRollbackLogs = true;

    /**
     * flags to globally enable/disable Statement.close() 
     */
    protected boolean currentActiveStatementCloseLogs = false;

    /**
     * default message filter per connection
     */
    protected CallMsgInfoFilter defaultConMsgFilter;

    protected HashSet<String> setDiscard = new HashSet<>();
    protected HashSet<String> lsDiscardPrefix = new HashSet<>();

    protected boolean slowSqlLogsEnable = true;
    protected int slowSqlThresholdMillis = 40000; // 40 seconds

    /**
     * flags to globally enable/disable all "ORA-0001" duplicate keys... !!!! 
     */
    protected boolean currentInterceptExceptionDuplicateKey = false;
    
    /**
     * 
     */
    private Map<String,TemplateSqlEntryStatsCallLogger> statsMap = new HashMap<String,TemplateSqlEntryStatsCallLogger>();

    private static class ConnIdInfo {
    	int connId;
    	String connectionUsername = null;
		String connectionDatabase = null;
    }
	// TODO... enhance ... this is for JdbcLoggerDataSource... each ds.getconnection() will re-wrap a vendor connection with JdbcLoggerConnection !!
    public static enum ConnIdInfoMode {
    	DummyIdGenerator,
    	SelectVendorId
    }
    private int dummyIdGenerator = 1;
    private ConnIdInfoMode connIdInfoMode = ConnIdInfoMode.SelectVendorId;
	private Map<Connection,ConnIdInfo> cache_connToConId = new HashMap<>();  

	private int newDummyId() {
		return ++dummyIdGenerator;
	}
	
    // ------------------------------------------------------------------------
    
    public ConnectionFactoryConfig(DriverConfigLog ownerDriver, int configId, String name, String connectionURL) {
        this.ownerDriver = ownerDriver;
        this.configId = configId;
        this.name = name;
        this.connectionURL = connectionURL;
        this.driverHelper = new DriverHelper(ownerDriver);
        loadProperties();
    }
    
    // ------------------------------------------------------------------------
 
	public int getConfigId() {
		return configId;
	}

    public String getName() {
        return name;
    }

    public String getConnectionURL() {
        return connectionURL;
    }
    
    private void loadProperties() {
        ClassLoader cl = getClass().getClassLoader();
        if (cl.getResource(RESOURCENAME_DISCARD) != null) {
            InputStream in = null;
            try {
                in = cl.getResourceAsStream(RESOURCENAME_DISCARD);
                FileUtil.readAllLines(in, setDiscard);
                // System.out.println("done read ressource for ignorable db statements, found " + setDiscard.size() + " elt(s)");
            } catch (Exception ex) {
            	//System.out.println("failed to read file for ignorable sql statement");
            } finally {
                if (in != null)
                    try {
                        in.close();
                    } catch (Exception ex) {
                    }
            }
        } else {
            //System.out.println("no resource named '" + RESOURCENAME_DISCARD + "' was found to ignore sql statements in jdbc logs");
        }

        if (cl.getResource(RESOURCENAME_DISCARDPREFIX) != null) {
            InputStream in = null;
            try {
                in = cl.getResourceAsStream(RESOURCENAME_DISCARDPREFIX);
                FileUtil.readAllLines(in, lsDiscardPrefix);
                // System.out.println("done read for ressource for ignorable db prefix statments, found " + lsDiscardPrefix.size() + " elt(s)");
            } catch (Exception ex) {
            	// System.out.println("failed to read file for ignorable db prefix-statment");
            } finally {
                if (in != null)
                    try {
                        in.close();
                    } catch (Exception ex) {
                    }
            }
        } else {
        	// System.out.println("no resource named '" + RESOURCENAME_DISCARD + "' was found to ignore sql prefix statements in jdbc logs");
        }

        this.properties = new Properties(ownerDriver.getDriverLoggerProperties());
        // TODO should override global properties by properties configured for URL / name
        // not implemented yet..
        
        int minDurationMillis = Integer.parseInt(properties.getProperty("minDurationMillis", "-1"));
        this.defaultConMsgFilter = new DefaultCallMsgInfoFilter(minDurationMillis, setDiscard, lsDiscardPrefix);

        this.currentActiveLogs = driverHelper.booleanProperty(properties, "activeLogs", currentActiveLogs);
        this.currentActiveCommitLogs = driverHelper.booleanProperty(properties, "activeCommitLogs",
                                                       currentActiveCommitLogs);
        this.currentActiveRollbackLogs = driverHelper.booleanProperty(properties, "activeRollbackLogs",
                                                         currentActiveRollbackLogs);

        // load class of underlying jdbc Driver class, to register driver class
        String underlyingDriverClass = (String) properties.get(UNDERLYING_DRIVER_CLASS);
        if (underlyingDriverClass != null) {
            registerUnderlyingDriver(underlyingDriverClass);
        }


        this.useTemplateSqlEntryStatsCallLogger = driverHelper.booleanProperty(properties, "useTemplateSqlEntryStatsCallLogger", true);

        
        this.currentInterceptExceptionDuplicateKey = driverHelper.booleanProperty(properties, "currentInterceptExceptionDuplicateKey", currentInterceptExceptionDuplicateKey);
        
        
        this.slowSqlLogsEnable = driverHelper.booleanProperty(properties, "slowSqlLogsEnable", slowSqlLogsEnable);
        this.slowSqlThresholdMillis = driverHelper.intProperty(properties, "slowSqlThresholdMillis", slowSqlThresholdMillis);
        
        
        CallMsgInfoFilter msgFilter = null; // TODO

        this.defaultCallMsgInfoLogger = new DefaultPendingCallLogger(this, "db", msgFilter, properties);

//        if (useDefaultConsoleLogger) {
//            this.defaultConsoleLogger = new DefaultConsoleCallLogger(this,
//                                                                     System.out,
//                                                                     msgFilter,
//                                                                     properties);
//        }

        if (useTemplateSqlEntryStatsCallLogger) {
            // setup the multi-threaded global TemplateRecorder CallLogger
            this.templateSqlEntryStatsCallLogger = new TemplateSqlEntryStatsCallLogger(this, properties);

            String jmxChildNameToExport = "Type=TemplateSqlEntryStats,ConfigId=" + configId;
            ownerDriver.tryJmxRegisterChildMBean(jmxChildNameToExport, templateSqlEntryStatsCallLogger);
        }
        this.log = ownerDriver.log;
    }
    
	public AbstractLoggerAdapter getLogger(Class<?> cls) {
		return ownerDriver.getLogger(cls.getName());
	}

	public AbstractLoggerAdapter getLogger(String logName) {
		return ownerDriver.getLogger(logName);
	}
	
	public DefaultPendingCallLogger getPendingCallLogger() {
	    DefaultPendingCallLogger res = null;
	    if (defaultCallMsgInfoLogger instanceof DefaultPendingCallLogger) {
	        res = (DefaultPendingCallLogger) defaultCallMsgInfoLogger;
	    }
	    return res; 
	}

	public TemplateSqlEntryStatsCallLogger getTemplateSqlEntryStatsCallLogger() {
		return templateSqlEntryStatsCallLogger;
	}
	
    public Properties getProperties() {
        return properties;
    }
    
    public void tryJmxRegisterChildMBean(String childName, Object mbean) {
        ownerDriver.tryJmxRegisterChildMBean(childName, mbean);
    }

    public void tryJmxUnregisterChildMBean(String childName) {
        ownerDriver.tryJmxUnregisterChildMBean(childName);
    }
        
    public Connection connect(String url, Properties props) throws SQLException {
        try {
            String targetUrl = "jdbc:" + url.substring("jdbc:log:".length());

            String underlyingDriverClass = (String) props.get(UNDERLYING_DRIVER_CLASS);
            if (underlyingDriverClass != null) {
                registerUnderlyingDriver(underlyingDriverClass);
            }

    		Properties loggerProperties = new Properties();
            Properties targetProps = new Properties();
            connPropertiesToUnderlyingConnProperties(props, targetProps, loggerProperties);

            Connection con = DriverManager.getConnection(targetUrl, targetProps);

            String openConnMsg = "connecting " + url;
			JdbcLoggerConnection res = wrapOpenConn(openConnMsg, con, loggerProperties, logOpenClose );

            return res;
        } catch (SQLException sqlEx) {
            defaultCallMsgInfoLogger.log("connect " + url + " FAILED:" + sqlEx.getMessage());
            log.error("connect " + url + " FAILED", sqlEx);
            throw sqlEx;
        } catch (Exception ex) {
            defaultCallMsgInfoLogger.log("connect " + url + " FAILED:" + ex.getMessage());
            log.error("connect " + url + " FAILED", ex);
            throw new RuntimeException(ex.getMessage());
        }
    }

	public void connPropertiesToUnderlyingConnProperties(Properties props, 
			Properties targetProps,
			Properties loggerProperties
			) {
		for (Map.Entry<Object,Object> e : props.entrySet()) {
		    String key = (String) e.getKey();
		    Object value = e.getValue();
		    if (key.startsWith(LOG_PROPERTY_PREFIX)) {
		        String logKey = key.substring(LOG_PROPERTY_PREFIX.length());
		        loggerProperties.put(logKey, value);
		    } else {
		        targetProps.put(key, value);
		    }
		}
	}

	private ConnIdInfo getConnIdInfo(Connection con, boolean logOpenClose) {
		ConnIdInfo connIdInfo;
		switch(connIdInfoMode) {
		case DummyIdGenerator:
			connIdInfo = new ConnIdInfo();
			connIdInfo.connId = newDummyId(); 
			break;
		case SelectVendorId:
			Connection underlyingCon = ConnectionUnwrapUtil.getUnderlyingConn(con);
			connIdInfo = cache_connToConId.get(underlyingCon);
	    	if (connIdInfo == null) {
	    		connIdInfo = new ConnIdInfo();
	    		connIdInfo.connId = driverHelper.retrieveConnId(underlyingCon);
	    		synchronized(cache_connToConId) {
	    			cache_connToConId.put(underlyingCon, connIdInfo);
	    			if (cache_connToConId.size() > 50) {
	    				log.info("**** detected too many cached info for connection->connId ... switching to mode dummyIdGenerator");
	    				cache_connToConId.clear();
	    				connIdInfoMode = ConnIdInfoMode.DummyIdGenerator;
	    			}
		        }
	    	}
			break;
		default:
			connIdInfo = new ConnIdInfo();
		}
		return connIdInfo;
	}

	public JdbcLoggerConnection wrapOpenConn(String openConnMsg, Connection con, Properties loggerProperties, boolean logOpenClose) {
		
		ConnIdInfo connIdInfo = getConnIdInfo(con, logOpenClose);
		
		JdbcLoggerConnection res = wrapConnLogger(con, connIdInfo.connId, loggerProperties, logOpenClose);

		
		if (logOpenClose) {
			String logMsg = openConnMsg
				+ " => database=" + connIdInfo.connectionDatabase
				+ ", user=" + connIdInfo.connectionUsername
				+ ", connId=" + connIdInfo.connId;
			
			onOpenConn(logMsg, res);
		}
		return res;
	}

	public JdbcLoggerXAConnection wrapOpenConn(String openConnMsg, XAConnection con, Properties loggerProperties, boolean logOpenClose) {

		ConnIdInfo connIdInfo; 
		try {
			Connection jdbcCon = con.getConnection();
			connIdInfo = getConnIdInfo(jdbcCon, logOpenClose);
		} catch (SQLException e) {
			// ignore.. no rethrow!
			connIdInfo = new ConnIdInfo();
		}
    			
		JdbcLoggerXAConnection res = (JdbcLoggerXAConnection) wrapConnLogger(con, connIdInfo.connId, loggerProperties, logOpenClose);
		
		if (logOpenClose) {
			String logMsg = openConnMsg
				+ " => database=" + connIdInfo.connectionDatabase
				+ ", user=" + connIdInfo.connectionUsername
				+ ", connId=" + connIdInfo.connId;
			
			onOpenConn(logMsg, res);
		}
		return res;
	}

    private static void registerUnderlyingDriver(String underlyingDriverClass) {
        if (underlyingDriverClass != null) {
            StringTokenizer tokenizer = new StringTokenizer(underlyingDriverClass, ";");
            for (; tokenizer.hasMoreElements();) {
                String elt = tokenizer.nextToken();
                try {
                    Class.forName(elt);
                    // System.out.println("Underlying JDBC driver class: " + elt + " registered.");
                } catch (ClassNotFoundException e) {
                	// System.out.println("JDBC driver class: " + elt + " not found, for instanciating/registering.", e);
                }
            }
        }
    }


    private JdbcLoggerConnection wrapConnLogger(Connection con, int connId, Properties loggerProperties, boolean logOpenClose) {
    	
    	// configure message logger / message filter per connection
        CallMsgInfoListLogger conLogHandler = new CallMsgInfoListLogger();

        conLogHandler.addAppender(ownerDriver.getCallLoggerList());

        conLogHandler.addAppender(callLoggerList);

        conLogHandler.addAppender(defaultCallMsgInfoLogger);

        if (templateSqlEntryStatsCallLogger != null) {
            conLogHandler.addAppender(templateSqlEntryStatsCallLogger);
        }

        JdbcLoggerConnection res = new JdbcLoggerConnection(this, conLogHandler, con, connId, loggerProperties, logOpenClose);

        return res;
    }

    public JdbcLoggerXAConnection wrapConnLogger(XAConnection con, int connId, Properties loggerProperties, boolean logOpenClose) {
    	// configure message logger / message filter per connection
        CallMsgInfoListLogger conLogHandler = new CallMsgInfoListLogger();

        conLogHandler.addAppender(ownerDriver.getCallLoggerList());

        conLogHandler.addAppender(callLoggerList);

        conLogHandler.addAppender(defaultCallMsgInfoLogger);

        if (templateSqlEntryStatsCallLogger != null) {
            conLogHandler.addAppender(templateSqlEntryStatsCallLogger);
        }

        JdbcLoggerXAConnection res = new JdbcLoggerXAConnection(con, this, conLogHandler, connId, loggerProperties, logOpenClose);
    	
        return res;
    }
    
    public void onOpenConn(String logMsg, JdbcLoggerConnection conn) {
        if (defaultCallMsgInfoLogger != null) {
            defaultCallMsgInfoLogger.log(logMsg);
        }
        log.info(logMsg);
    }

    public void onCloseConn(JdbcLoggerConnection conn, boolean logOpenClose) {
        if (logOpenClose) {
	    	String logMsg = "close conn " + conn.getConnectionId();
	        if (defaultCallMsgInfoLogger != null) {
	            defaultCallMsgInfoLogger.log(logMsg);
	        }
	        log.info(logMsg);
        }
    }

    public void onOpenConn(String logMsg, JdbcLoggerXAConnection conn) {
        if (defaultCallMsgInfoLogger != null) {
            defaultCallMsgInfoLogger.log(logMsg);
        }
        log.info(logMsg);
    }

    public void onCloseConn(JdbcLoggerXAConnection conn) {
        String logMsg = "close conn " + conn.getConnectionId();
        if (defaultCallMsgInfoLogger != null) {
            defaultCallMsgInfoLogger.log(logMsg);
        }
        log.info(logMsg);
    }

    
    // implements ConnectionFactoryMBean
    // ------------------------------------------------------------------------

    /** implements DriverLogMBean */
    public boolean isCurrentActiveLogs() {
        return currentActiveLogs;
    }

    /** implements DriverLogMBean */
    public void setCurrentActiveLogs(boolean p) {
        log.info("setCurrentActiveLogs " + p);
        this.currentActiveLogs = p;
    }

    /** implements DriverLogMBean */
    public boolean isCurrentActiveCommitLogs() {
        return currentActiveCommitLogs;
    }

    /** implements DriverLogMBean */
    public void setCurrentActiveCommitLogs(boolean p) {
        log.info("setCurrentActiveCommitLogs " + p);
        this.currentActiveCommitLogs = p;
    }

    /** implements DriverLogMBean */
    public boolean isCurrentActiveRollbackLogs() {
        return currentActiveRollbackLogs;
    }

    /** implements DriverLogMBean */
    public void setCurrentActiveRollbackLogs(boolean p) {
        log.info("setCurrentActiveRollbackLogs " + p);
        this.currentActiveRollbackLogs = p;
    }

    /** implements DriverLogMBean */
    public boolean isCurrentActiveStatementCloseLogs() {
    	return currentActiveStatementCloseLogs;
    }
    
    /** implements DriverLogMBean */
    public void setCurrentActiveStatementCloseLogs(boolean p) {
        log.info("setCurrentActiveStatementCloseLogs " + p);
    	this.currentActiveStatementCloseLogs = p;
    }

    /** implements DriverLogMBean */    
    public boolean isCurrentInterceptExceptionDuplicateKey() {
        return currentInterceptExceptionDuplicateKey;
    }

    /** implements DriverLogMBean */
    public void setCurrentInterceptExceptionDuplicateKey(boolean p) {
        this.currentInterceptExceptionDuplicateKey = p;
    }

    /** implements DriverLogMBean */
    public boolean isSlowSqlLogsEnable() {
		return slowSqlLogsEnable;
	}

    /** implements DriverLogMBean */
	public void setSlowSqlLogsEnable(boolean p) {
		this.slowSqlLogsEnable = p;
	}

    /** implements DriverLogMBean */
	public int getSlowSqlThresholdMillis() {
		return slowSqlThresholdMillis;
	}

    /** implements DriverLogMBean */
	public void setSlowSqlThresholdMillis(int p) {
		this.slowSqlThresholdMillis = p;
	}

	/** implements DriverLogMBean */
    public int getStatCount() {
        return statsMap.size();
    }

    /** implements DriverLogMBean */
    public String getStatNames() {
        StringBuilder sb = new StringBuilder();
        for (String elt : statsMap.keySet()) {
            sb.append(elt);
            sb.append("\n");
        }
        return sb.toString();
    }

    /** implements DriverLogMBean */
    public void addStats(String name) {
        log.info("addStats name=" + name);

        TemplateSqlEntryStatsCallLogger statsLogger = new TemplateSqlEntryStatsCallLogger(this, properties);

        String jmxChildName = "Type=TemplateSqlEntryStats,ConfigId=" + configId;
        ownerDriver.tryJmxRegisterChildMBean(jmxChildName, statsLogger);

        callLoggerList.addAppender(statsLogger);
        statsMap.put(name, statsLogger);
    }

    /** implements DriverLogMBean */
    public void removeStats(String name) {
        log.info("removeStats name=" + name);
        TemplateSqlEntryStatsCallLogger statsLogger = statsMap.remove(name);
        if (statsLogger != null) {
            callLoggerList.removeAppender(statsLogger);

            String jmxChildName = "Type=TemplateSqlEntryStats,Name=" + name;
            ownerDriver.tryJmxUnregisterChildMBean(jmxChildName);
        } else {
            log.info("stats not found ... nothing to remove");
        }
    }

    /** implements DriverLogMBean */
    public void clearStatSqlStats(String name) {
        log.info("clearStatSqlStats name=" + name);
        TemplateSqlEntryStatsCallLogger statsLogger = statsMap.get(name);
        if (statsLogger != null) {
            statsLogger.clearSqlStats();
        } else {
            log.info("stats not found ... do nothing");
        }
    }

    /** helper for TemplateSqlEntryStatsCallLoggerMBean */
    public void clearStatTemplateSqlEntries(String name) {
        log.info("clearStatTemplateSqlEntries name=" + name);
        TemplateSqlEntryStatsCallLogger statsLogger = statsMap.get(name);
        if (statsLogger != null) {
            statsLogger.clearTemplateSqlEntries();
        } else {
            log.info("stats not found ... do nothing");
        }
    }

    /** implements DriverLogMBean */
    public void dumpCsvStatsFile(String name, String dumpFileName) {
        log.info("dumpCsvStatsFile name=" + name);
        TemplateSqlEntryStatsCallLogger statsLogger = statsMap.get(name);
        if (statsLogger != null) {
            statsLogger.writeCsvFileDumpSqlStats2(dumpFileName);
        } else {
            log.info("stats not found ... do nothing");
        }
    }

    /** helper for TemplateSqlEntryStatsCallLoggerMBean */
    public String getCsvDumpStats(String name) {
        // log.info("getCsvDumpStats name=" + name);
        String res;
        TemplateSqlEntryStatsCallLogger statsLogger = statsMap.get(name);
        if (statsLogger != null) {
            res = statsLogger.getCsvDumpSqlStats();
        } else {
            log.info("stats not found ... do nothing");
            res = "";
        }
        return res;
    }

    /** helper for TemplateSqlEntryStatsCallLoggerMBean */
    public String[][] getArrayDumpStats(String name) {
        String[][] res;
        TemplateSqlEntryStatsCallLogger statsLogger = statsMap.get(name);
        if (statsLogger != null) {
            res = statsLogger.getArrayDumpSqlStats();
        } else {
            log.info("stats not found ... do nothing");
            res = new String[0][];
        }
        return res;
    }
    
}
