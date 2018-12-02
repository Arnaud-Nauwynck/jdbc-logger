package fr.an.tools.jdbclogger.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import fr.an.tools.jdbclogger.DriverLog;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoListLogger;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoLogger;
import fr.an.tools.jdbclogger.calllogger.templaterecorder.TemplateSqlEntryStatsCallLogger;
import fr.an.tools.jdbclogger.util.JMXExporterHelper;
import fr.an.tools.jdbclogger.util.logadapter.AbstractLoggerAdapter;
import fr.an.tools.jdbclogger.util.logadapter.CommonsLoggerAdapter;
import fr.an.tools.jdbclogger.util.logadapter.FileLoggerAdapter;
import fr.an.tools.jdbclogger.util.logadapter.JULLoggerAdapter;
import fr.an.tools.jdbclogger.util.logadapter.Log4jLoggerAdapter;
import fr.an.tools.jdbclogger.util.logadapter.LoggerAdapterFactory;
import fr.an.tools.jdbclogger.util.logadapter.Slf4jLoggerAdapter;

/**
 * java.sql.Driver implementation returning Connection wrapped for logging
 * see ConnectionLog
 * 
 * to write CSV files dump of stats:
 * <PRE>
 * DriverConfigLog.writeCsvFileDumpSqlStats("dump");
 * </PRE>
 */
public class DriverConfigLog implements Driver, DriverConfigLogMBean {

    public static final String RESOURCENAME_PROPERTIES = "jdbclogger-config.properties";

    public static final String LOG_PROPERTY_PREFIX = "log_";
    
    public static final String LOGGER_CLASSNAMES_PROPKEY = "LoggerClassNames";
        
    // ------------------------------------------------------------------------


    /** properties for logging in Driver */
    protected Properties driverLoggerProperties = new Properties();
    
    protected LoggerAdapterFactory loggerAdapterFactory;
    
    protected AbstractLoggerAdapter log; 
    
    private int configIdGenerator = 1;

    private Map<String,ConnectionFactoryConfig> connectionFactoryConfigMap = new HashMap<String,ConnectionFactoryConfig>();
    
    /**
     * composite list of CallLogger added at runtime for this Driver (=> for all child ConnectionFactoryConfig)
     */
    protected CallMsgInfoListLogger callLoggerList = new CallMsgInfoListLogger();

    
    private JMXExporterHelper jmxExporterHelper;
    
    /**
     * for JMX: base domain name to export MBeans 
     */
    private String jmxMbeanDomainToExport;

    /**
     * for JMX
     */
    private String jmxBaseNameToExportDriver;
    
	
	
    // ------------------------------------------------------------------------
    
    public DriverConfigLog() {
        initialize();
    }
	
    private void initialize() {
        ClassLoader cl = DriverConfigLog.class.getClassLoader();

        System.out.println("***** configure Jdbc-Logger Driver instance *****");

        InputStream propStream = null;
        try {
            if (propStream == null) {
            	// searching in CLASSPATH
                propStream = cl.getResourceAsStream(RESOURCENAME_PROPERTIES);
                if (propStream != null) {
                	System.out.println("reading jdbc-logger properties file from CLASSPATH resource:" + RESOURCENAME_PROPERTIES);
                }
            }
            if (propStream == null) {
            	// searching in current working directory
            	File f = new File(RESOURCENAME_PROPERTIES);
            	if (f.exists() && f.canRead()) {
            		propStream = new FileInputStream(f);
                	System.out.println("reading jdbc-logger properties file from current working dir:" + f.getAbsolutePath());
            	}
            }
            if (propStream == null) {
            	// searching in current working directory
            	File f = new File("config/" + RESOURCENAME_PROPERTIES);
            	if (f.exists() && f.canRead()) {
            		propStream = new FileInputStream(f);
                	System.out.println("reading jdbc-logger properties file from config/ dir:" + f.getAbsolutePath());
            	}
            }
            if (propStream == null) {
            	// searching in current working directory
            	File f = new File("src/test/resources/" + RESOURCENAME_PROPERTIES);
            	if (f.exists() && f.canRead()) {
            		propStream = new FileInputStream(f);
                	System.out.println("reading jdbc-logger properties file from current test dir:" + f.getAbsolutePath());
            	}
            }
            
            driverLoggerProperties.load(propStream);
            // System.out.println("succesfully read jdbc-logger properties: " + driverLoggerProperties);
        } catch (Exception ex) {
            String msg = "failed to read logger properties file " + RESOURCENAME_PROPERTIES;
            System.err.println(msg);
        } finally {
            if (propStream != null) try { propStream.close(); } catch (Exception ex) { }
        }


        // Setup Log system (Slf4j, Log4j, Direct java.io.PrintStream, commons-logging, JUL ...)
        String logSystem = driverLoggerProperties.getProperty("logSystem", "slf4j");
        try {
	        if (logSystem.equalsIgnoreCase("slf4j")) {
	        	loggerAdapterFactory = new Slf4jLoggerAdapter.Slf4jLoggerAdapterFactory();
	        } else if (logSystem.equalsIgnoreCase("log4j")) {
	        	loggerAdapterFactory = new Log4jLoggerAdapter.Log4jLoggerAdapterFactory();
	        } else if (logSystem.equalsIgnoreCase("jul")) {
	        	loggerAdapterFactory = new JULLoggerAdapter.JULLoggerAdapterFactory();
	        } else if (logSystem.equalsIgnoreCase("commons-logging")) {
	        	loggerAdapterFactory = new CommonsLoggerAdapter.CommonsLoggerAdapterFactory();
	        } else if (logSystem.equalsIgnoreCase("file")) {
	        	loggerAdapterFactory = new FileLoggerAdapter.FileLoggerAdapterFactory("jdbclog.txt");
	        } else if (logSystem.equalsIgnoreCase("stdout")) {
	        	loggerAdapterFactory = new FileLoggerAdapter.FileLoggerAdapterFactory("stdout");
	        } else {
	        	loggerAdapterFactory = new FileLoggerAdapter.FileLoggerAdapterFactory("jdbclog.txt");
	        }
        } catch(Exception ex) {
        	loggerAdapterFactory = new FileLoggerAdapter.FileLoggerAdapterFactory("jdbclog.txt");
        }
        this.log = loggerAdapterFactory.getLogger(getClass().getName());
        
        
        String loggerClassNames = driverLoggerProperties.getProperty(LOGGER_CLASSNAMES_PROPKEY); // , LOGGER_CLASSNAMES_DEFAULT_VALUE);
        if (loggerClassNames != null && loggerClassNames.trim().length() != 0) {
        	String[] classNames = loggerClassNames.split(",");
        	for(String cn : classNames) {
        		System.out.println("registering jdbclogger new CallLogger for class '" + cn + "'");
        		try {
        			Class<?> clss = Class.forName(cn);
        			CallMsgInfoLogger callLogger = (CallMsgInfoLogger) clss.newInstance();
        			callLoggerList.addAppender(callLogger);
        		} catch(Exception ex) {
        			System.out.println("Failed to add CallMsgInfoLogger new instance for class " + cn);
        		}
        	}
        }
        
        jmxExporterHelper = new JMXExporterHelper(this);
        
        // JMX export for controlling this Driver
        jmxExporterHelper.init(driverLoggerProperties);

        this.jmxMbeanDomainToExport = driverLoggerProperties.getProperty("jmxMbeanDomainToExport",
                                                                         "fr.an.tools.jdbc.proxylogger");

        this.jmxBaseNameToExportDriver = driverLoggerProperties.getProperty("jmxBaseNameToExportDriver",
                                                                            "Type=JdbcLoggerDriver,Name=Default");
        try {
            String beanName = jmxMbeanDomainToExport + ":" + jmxBaseNameToExportDriver;
            jmxExporterHelper.registerMBean(this, beanName);
        } catch (Exception ex) {
            log.warn("failed to export JdbcLogger Driver as MBean to MBeanServer ... Driver will not be controlable by JMX, ignore ex..."
                     + ex.getMessage());
            // ignore, no rethrow
        }
        
    }

    // implements java.sql.Driver, delegate to target
    // ------------------------------------------------------------------------

    /** implements java.sql.jdbc.Driver */
    public boolean acceptsURL(String url) throws SQLException {
        boolean res;
        if (url.startsWith("jdbc:log:")) {
            res = true;
        } else {
            res = false;
        }
        return res;
    }

    /** implements java.sql.jdbc.Driver */
    public Connection connect(String url, Properties props) throws SQLException {
        if (url.startsWith("jdbc:log:")) {
            ConnectionFactoryConfig config = getConnectionFactoryConfig(url);
            Connection res = config.connect(url, props);
            return res;
        } else {
            return null;
        }
    }

	public ConnectionFactoryConfig getConnectionFactoryConfig(String url) {
		ConnectionFactoryConfig config = connectionFactoryConfigMap.get(url);
		if (config == null) {
			// redo with synchronized.. 
			synchronized(connectionFactoryConfigMap) {
				config = connectionFactoryConfigMap.get(url);
				if (config == null) {
		        	connectionFactoryConfigMap.get(url);
		            String name = url; // TOCHANGE?? 
		            int configId = configIdGenerator++;
		            config = new ConnectionFactoryConfig(this, configId, name, url);
		            connectionFactoryConfigMap.put(url, config);
		            
		            String childMBeanName = "Type=ConnectionURL,ConfigId=" + configId;
		            tryJmxRegisterChildMBean(childMBeanName, config);
				}
			}
			
		}
		return config;
	}

    
    /** implements java.sql.jdbc.Driver */
    public int getMajorVersion() {
        return 1;
    }

    /** implements java.sql.jdbc.Driver */
    public int getMinorVersion() {
        return 0;
    }

    /** implements java.sql.jdbc.Driver */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    /** implements java.sql.jdbc.Driver */
    public boolean jdbcCompliant() {
        return true;
    }

    // ------------------------------------------------------------------------
    
    public Properties getDriverLoggerProperties() {
        return driverLoggerProperties;
    }

	public CallMsgInfoListLogger getCallLoggerList() {
		return callLoggerList;
	}

    public AbstractLoggerAdapter getLogger(Class<?> clss) {
    	return this.loggerAdapterFactory.getLogger(clss.getName());
    }

    public AbstractLoggerAdapter getLogger(String className) {
    	return this.loggerAdapterFactory.getLogger(className);
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger(DriverLog.class.getName());
    }
    
    // JMX
    // ------------------------------------------------------------------------
    

    public String getJmxMbeanDomainToExport() {
        return jmxMbeanDomainToExport;
    }

    /*pp*/ void tryJmxRegisterChildMBean(String childName, Object mbean) {
        String objectName = jmxMbeanDomainToExport + ":" + childName;
        try {
        	jmxExporterHelper.registerMBean(mbean, objectName);
        } catch (Exception ex) {
            log.warn("failed to register jmx MBean " + mbean + " with name '" + childName
                     + "' ... object will not be controllable by JMX ... ignore, no rethrow", ex);
            // ignore, no rethrow
        }
    }

    /*pp*/ void tryJmxUnregisterChildMBean(String childName) {
        String objectName = jmxMbeanDomainToExport + ":" + childName;
        try {
        	jmxExporterHelper.unregisterMBean(objectName);
        } catch (Exception ex) {
            log.warn("failed to unregister jmx MBean with name '" + childName + "' ... ignore, no rethrow", ex);
            // ignore, no rethrow
        }
    }

    public Map<String,ConnectionFactoryConfig> getConnectionFactoryConfigMapCopy() {
        return new HashMap<String,ConnectionFactoryConfig>(connectionFactoryConfigMap);        
    }

    public static void writeCsvFileDumpSqlStats(String filePrefix) {
	    DriverConfigLog driver = DriverLog.getDefaultInstance();
	    Map<String,ConnectionFactoryConfig> m = driver.getConnectionFactoryConfigMapCopy();
	    for(ConnectionFactoryConfig c : m.values()) {
		    TemplateSqlEntryStatsCallLogger templateSqlEntryStatsCallLogger =  c.getTemplateSqlEntryStatsCallLogger();
		    if (templateSqlEntryStatsCallLogger != null) {
		    	templateSqlEntryStatsCallLogger.writeCsvFileDumpSqlStats2(filePrefix + "-" + c.getConfigId() + ".csv");
		    }
	    }
    }

}
