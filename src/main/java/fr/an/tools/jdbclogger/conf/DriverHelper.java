package fr.an.tools.jdbclogger.conf;

import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;

import fr.an.tools.jdbclogger.DriverLog;
import fr.an.tools.jdbclogger.util.ReflectUtils;
import fr.an.tools.jdbclogger.util.logadapter.AbstractLoggerAdapter;

public class DriverHelper {

	protected DriverConfigLog ownerDriver;
	
    private final AbstractLoggerAdapter log;

    /** used only for unkown driver, or when sql to get id fails.. */
    private int connectionIdGenerator = 1;

    private static final String ORACLE_SELECT_CONNID = "select sys_context('USERENV', 'SESSIONID') FROM dual";
	private static final String ORACLE_SELECT_USERNAME = "select SYS_CONTEXT('USERENV', 'SESSION_USER') from dual";
	private static final String ORACLE_SELECT_DATABASENAME = "select name from v$database";

	private static final Class<?> CLASS_ORNULL_OracleConnection = ReflectUtils.safeLoadClassOrNull("oracle.jdbc.driver.OracleConnection");
	
	private Map<Connection, Integer> conn2id = new WeakHashMap<Connection, Integer>();  
	
    // ------------------------------------------------------------------------

    public DriverHelper(DriverConfigLog ownerDriver) {
		this.ownerDriver = ownerDriver;
		this.log = ownerDriver.getLogger(getClass());
	}
    
    /** utility helper */
    public boolean booleanProperty(Properties props, String propertyName, boolean defaultValue) {
        boolean res = defaultValue;
        String value = props.getProperty(propertyName);
        if (value != null) {
            value = value.trim();
            try {
                res = Boolean.parseBoolean(value);
            } catch (Exception ex) {
                res = defaultValue;
                log.error("failed to parse boolean property '" + propertyName + "' = '" + value + "' .. use default:"
                          + defaultValue);
                // ignore, no rethrow!
            }
        }
        return res;
    }

    /** utility helper */
    public int intProperty(Properties props, String propertyName, int defaultValue) {
        int res = defaultValue;
        String value = props.getProperty(propertyName);
        if (value != null) {
            value = value.trim();
            try {
                res = Integer.parseInt(value);
            } catch (Exception ex) {
                res = defaultValue;
                log.error("failed to parse int property '" + propertyName + "' = '" + value + "' .. use default:"
                          + defaultValue);
                // ignore, no rethrow!
            }
        }
        return res;
    }
    

	/** utility helper */
    public void checkResource(ClassLoader cl, String resourceName) {
        try {
            System.out.println("checking resource '" + resourceName + "' in classpath");
            URL url = cl.getResource(resourceName);
            if (url == null) {
                System.out.println("'" + resourceName + "' not found in classPath resources!");
            } else {
                System.out.println("found resource for '" + resourceName + "' in classPath :'" + url + "'");
            }
        } catch (Exception ex) {
            System.out.println("Failed to find '" + resourceName + "' in classpath");
        }
    }


    public int retrieveConnId(Connection con) {
        int connId = -1;
        
        Integer tmpres = conn2id.get(con);
        if (tmpres != null) {
            return tmpres.intValue(); 
        }
        
        try {
        	String qry;
        	Connection underlyingConn = ConnectionUnwrapUtil.getUnderlyingConn(con);
            if (connId < 0 
            		&& CLASS_ORNULL_OracleConnection != null && CLASS_ORNULL_OracleConnection.isInstance(underlyingConn)  // idem.. underlyingConn instanceof OracleConnection
                    ) {
            	// try Oracle specific...
                try {
                	qry = ORACLE_SELECT_CONNID;
                	connId = executeQueryInt(con, qry);
                    log.info("get connection id: '" + qry + "' returned " + connId);
                } catch(Exception ex) {
                	// ignore, no rethrow??!!  (not oracle?)
                }
            }

            // don't want to try other ...
            
            if (connId < 0) {
                synchronized (DriverLog.class) {
                    connId = connectionIdGenerator++;
                }
                log.info("get connection id?? (only oracle/ is supported here), incrementing internal counter " + connId);
            }

        } catch (Exception ex) {
            synchronized (DriverLog.class) {
                connId = connectionIdGenerator++;
            }
            log.warn("Failed to get connection id, incrementing internal counter " + connId);
            // ignore, no rethrow
        }
        
        conn2id.put(con, connId);
        log.info("****** register connId: " + connId + " for conn:" + con);

        return connId;
    }

    public String retrieveUsernameConnection(Connection con) {
        String qry = ORACLE_SELECT_USERNAME;
        String res = executeQueryString(con, qry);
        return res;
    }

    public String retrieveDatabasename(Connection con) {
        String qry = ORACLE_SELECT_DATABASENAME; 
        String res = executeQueryString(con, qry);
        return res;
    }
    
    

    public int executeQueryInt(Connection con, String qry) throws SQLException {
        int res = -1;
        PreparedStatement stmt = null;
        try {
            stmt = con.prepareStatement(qry);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                res = rs.getInt(1);
            } else {
                res = -1; // should not occur!
            }
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ex) {
                }
            }
        }
        return res;
    }

    public String executeQueryString(Connection con, String qry) {
        String res = null; 
        PreparedStatement stmt = null;
        try {
            stmt = con.prepareStatement(qry);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            res = rs.getString(1);
        } catch(SQLException ex) {
            res = null;
            log.warn("Failed to execute qry " + qry);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ex) {
                }
            }
        }
        return res;
    }
    
    
}
