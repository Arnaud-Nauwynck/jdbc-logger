package fr.an.tools.jdbclogger.client;

import javax.sql.XADataSource;

import fr.an.tools.jdbclogger.DriverLog;
import fr.an.tools.jdbclogger.conf.DriverConfigLog;
import fr.an.tools.jdbclogger.util.ReflectUtils;

/**
 *
 */
public class JdbcLoggerBeanXADataSource extends JdbcLoggerProxyXADataSource implements XADataSource {

    private static final String DEFAULT_CLASSNAME_OracleXADataSource = "oracle.jdbc.xa.client.OracleXADataSource";

    protected String dataSourceClass = DEFAULT_CLASSNAME_OracleXADataSource;
    
	private String url;
	
	private String user;
	
	private String password;
	
	// ------------------------------------------------------------------------

	public JdbcLoggerBeanXADataSource() {
	}

	// ------------------------------------------------------------------------
	
	public XADataSource getTarget() {
		if (target == null) {
			target = doCreateTargetDataSource();
		}
		return target;
	}

	
	private XADataSource doCreateTargetDataSource() {
		XADataSource res = null;
		
		DriverConfigLog driverLog = DriverLog.getDefaultInstance(); 
		this.connectionFactoryConfig = driverLog.getConnectionFactoryConfig(url);
		
		connectionFactoryConfig.tryJmxRegisterChildMBean("type=XADatasource", this);
		
        String targetUrl = url.replace("jdbc:log:", "jdbc:");  
		if (url.startsWith("jdbc:log:oracle:") || url.startsWith("jdbc:oracle:")) {  
		    if (dataSourceClass == null || DEFAULT_CLASSNAME_OracleXADataSource.equals(dataSourceClass)) {
				res = ReflectUtils.newInstance("oracle.jdbc.xa.client.OracleXADataSource");
				ReflectUtils.invokeMethod(res, "setURL", String.class, targetUrl);
				ReflectUtils.invokeMethod(res, "setUser", String.class, user);
				ReflectUtils.invokeMethod(res, "setPassword", String.class, password);
		    } else {
		        // unrecognized hard-coded oracle...
		    }
		}

		if (res == null) {
			throw new RuntimeException("Failed to create Datasource .. NOT IMPLEMENTED YET");
		}
		return res;
	}

	// ------------------------------------------------------------------------

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	// usefull fields getter/setter alias
	// ------------------------------------------------------------------------
	
	/** alias for getUrl() */
	public String getURL() {
		return getUrl();
	}

	/** alias for setUrl() */
	public void setURL(String url) {
		setUrl(url);
	}

	/** alias for getUser() */
	public String getLogin() {
		return getUser();
	}

	/** alias for setUser() */
	public void setLogin(String login) {
		setUser(login);
	}

	/** alias for getUser() */
	public String getUsername() {
		return getUser();
	}

	/** alias for setUser() */
	public void setUsername(String p) {
		setUser(p);
	}

}
