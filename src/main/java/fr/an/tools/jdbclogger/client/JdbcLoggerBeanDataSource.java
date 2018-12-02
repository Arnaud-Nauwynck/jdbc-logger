package fr.an.tools.jdbclogger.client;


import java.sql.Driver;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import fr.an.tools.jdbclogger.util.ReflectUtils;

/**
 * Helper class for building a DataSource from parameters, and wrapping it with JdbcLogger
 * 
 * @see org.springframework.jdbc.datasource.SimpleDriverDataSource
 * @see oracle.jdbc.xa.client.OracleXADataSource
 */
public class JdbcLoggerBeanDataSource extends JdbcLoggerProxyDataSource {

    private static final String DEFAULT_CLASSNAME_OracleDataSource = "oracle.jdbc.pool.OracleDataSource";
    private static final String DEFAULT_CLASSNAME_OracleXADataSource = "oracle.jdbc.xa.client.OracleXADataSource";
    private static final String DEFAULT_CLASSNAME_OracleDriver = "oracle.jdbc.driver.Driver";
    
	protected String dataSourceClass = DEFAULT_CLASSNAME_OracleDataSource;
	protected String driverClass = DEFAULT_CLASSNAME_OracleDriver;
	
	protected String url;
	protected String user;
	protected String password;
	
	protected String initSql;
	
	// ------------------------------------------------------------------------
	
	public JdbcLoggerBeanDataSource() {
		super();
	}

	// ------------------------------------------------------------------------
	

	public void init() {
	}
	
	/** override to lazy initializing it.. */
	public DataSource getTarget() {
	    if (target == null) {
            target = doCreateTargetDataSource();
        }
        return target;
    }

    
    protected DataSource doCreateTargetDataSource() {        
        DataSource res = null;
        if (res == null) {
            String targetUrl = url.replace("jdbc:log:", "jdbc:");
			if (url.startsWith("jdbc:log:oracle:") || url.startsWith("jdbc:oracle:")) {
			    
			    if (DEFAULT_CLASSNAME_OracleDataSource.equals(dataSourceClass)) { 
					res = ReflectUtils.newInstance("oracle.jdbc.pool.OracleDataSource");
					ReflectUtils.invokeMethod(res, "setURL", String.class, targetUrl);
					ReflectUtils.invokeMethod(res, "setUser", String.class, user);
					ReflectUtils.invokeMethod(res, "setPassword", String.class, password);
			    } else if (DEFAULT_CLASSNAME_OracleXADataSource.equals(dataSourceClass)) {
					res = ReflectUtils.newInstance("oracle.jdbc.xa.client.OracleXADataSource");
					ReflectUtils.invokeMethod(res, "setURL", String.class, targetUrl);
					ReflectUtils.invokeMethod(res, "setUser", String.class, user);
					ReflectUtils.invokeMethod(res, "setPassword", String.class, password);
			    } else {
			        
			    }
			}

			if (res == null && dataSourceClass == null && driverClass != null) {
				// unrecognized DataSource ??? 
				// => use generic Spring DriverDataSource ... NOT XA !!!
				SimpleDriverDataSource ds = new SimpleDriverDataSource();
				
				Class<Driver> driverClss;
				try {
					@SuppressWarnings("unchecked")
					Class<Driver> tmpClss = (Class<Driver>) Class.forName(driverClass);
					driverClss = tmpClss;
				} catch(Exception ex) {
					throw new RuntimeException("Failed to load Class.forName " + driverClass, ex);
				}
				ds.setDriverClass(driverClss);

				ds.setUrl(targetUrl);
				ds.setUsername(user);
				ds.setPassword(password);
				
				res = ds;
			}

            if (res == null && dataSourceClass != null) {
                // TODO try to create DataSource by introspection + configure available properties...
            }

            if (res == null) {
			    throw new RuntimeException("Failed to create Datasource .. NOT IMPLEMENTED YET");
			}

		}
		return res;
	}
	

	// Getters, Setters
	// ------------------------------------------------------------------------
	
	public String getDriverClass() {
		return driverClass;
	}

	public void setDriverClass(String driverClass) {
		this.driverClass = driverClass;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String p) {
		this.url = p;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String p) {
		this.user = p;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getInitSql() {
		return initSql;
	}

	public void setInitSql(String initSql) {
		this.initSql = initSql;
	}

	// commonly used Alias for getter/setters 
	// ------------------------------------------------------------------------

	public String getUsername() {
		return getUser();
	}

	public void setUsername(String p) {
		setUser(p);
	}

	public String getLogin() {
		return getUser();
	}

	public void setLogin(String p) {
		setUser(p);
	}

	public String getURL() {
		return getUrl();
	}

	public void setURL(String p) {
		setUrl(p);
	}

	public String getJdbcUrl() {
		return getUrl();
	}

	public void setJdbcUrl(String p) {
		setUrl(p);
	}
	
}
