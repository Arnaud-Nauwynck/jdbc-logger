package fr.an.tools.jdbclogger;

import fr.an.tools.jdbclogger.conf.DriverConfigLog;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * java.sql.Driver entry-point implementation for jdbc-logger
 * unsafe "singleton" anti-pattern (client code is public and might be instantiate this class several times...)
 * => delegate all to DriverConfigLog singleton 
 * 
 * Sample debug code to dump ...
 * <PRE>
fr.an.tools.jdbclogger.conf.DriverConfigLog driver = fr.an.tools.jdbc.proxylogger.DriverLog.getDefaultInstance();
java.util.Map m = driver.getConnectionFactoryConfigMapCopy();
fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig c = (fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig) 
	// m.get("jdbc:oracle:thin:@XXX:YYY:ZZZ");
	m.values().iterator().next();
fr.an.tools.jdbclogger.calllogger.templaterecorder.TemplateSqlEntryStatsCallLogger templateSqlEntryStatsCallLogger =  c.templateSqlEntryStatsCallLogger;
templateSqlEntryStatsCallLogger.writeCsvFileDumpSqlStats2("dump.csv");
 </PRE>
 
 */
public class DriverLog implements Driver {

    private static DriverConfigLog defaultInstance = new DriverConfigLog();
    
    
    // static init
    // ------------------------------------------------------------------------

    static {
        try {
            System.out.println("******* <cinit> registerDrivers");
            registerDrivers();
        } catch(Exception ex) {
            System.out.println("******* FAILED <cinit> registerDrivers");
        }
    }
    

    public static void registerDrivers() {
        // do register Driver class/instance to DriverManager ???? (should be done by caller?)
        try {
            System.out.println("********* autoregistering fr.an.tools.jdbc.proxylogger.DriverLog");
            DriverManager.registerDriver(defaultInstance);
        } catch (Exception ex) {
            System.err.println("Failed to register driver!");
            ex.printStackTrace(System.err);
        }

        // try to auto-register common drivers...
        checkOrRegisterDriverForClass("oracle.jdbc.OracleDriver");
        checkOrRegisterDriverForClass("org.h2.Driver");
        checkOrRegisterDriverForClass("org.postgresql.Driver");

    }


    private static void checkOrRegisterDriverForClass(String className) {
    	try {
            if (null == findDriverForClass(className)) {
                System.out.println("*********** auto registering driver '" + className + "'");
				Class clss = Class.forName(className);
	            Driver driver = (Driver) clss.newInstance();
	            DriverManager.registerDriver(driver);
            } else {
                System.out.println("******** driver '" + className + "' already registered");
            }
        } catch(Exception ex) {
            // ignore, no rethrow!
            System.err.println("*********** failed to register driver '" + className + "' ... ignore");
            ex.printStackTrace(System.err);
        }
    }
    
	private static Driver findDriverForClass(String className) {
		Driver found = null;
		for(java.util.Enumeration<Driver> iter = DriverManager.getDrivers(); iter.hasMoreElements(); ) {
			Driver elt = iter.nextElement();
			if (elt.getClass().getName().equals(className)) {
				found = elt;
				break;
			}
		}
		return found;
	}
	
    // ------------------------------------------------------------------------

	/** empty ctor ... not a real singleton ==> delegate all to defaultInstance */
    public DriverLog() {
    }

    public static DriverConfigLog getDefaultInstance() {
    	return defaultInstance; 
    }
    
    
    
    // implements java.sql.Driver, delegate to defaultInstance
    // ------------------------------------------------------------------------

    /** implements java.sql.jdbc.Driver */
    public boolean acceptsURL(String url) throws SQLException {
        return defaultInstance.acceptsURL(url);
    }

    /** implements java.sql.jdbc.Driver */
    public Connection connect(String url, Properties props) throws SQLException {
        return defaultInstance.connect(url, props);
    }
    
    /** implements java.sql.jdbc.Driver */
    public int getMajorVersion() {
    	return defaultInstance.getMajorVersion();
    }

    /** implements java.sql.jdbc.Driver */
    public int getMinorVersion() {
    	return defaultInstance.getMinorVersion();
    }

    /** implements java.sql.jdbc.Driver */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    	return defaultInstance.getPropertyInfo(url, info);
    }

    /** implements java.sql.jdbc.Driver */
    public boolean jdbcCompliant() {
    	return defaultInstance.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger(DriverLog.class.getName());
    }

    
}
