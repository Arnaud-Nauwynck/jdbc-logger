package fr.an.tools.jdbclogger.client;

import fr.an.tools.jdbclogger.DriverLog;
import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;

import javax.sql.CommonDataSource;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Properties;

public abstract class AbstractJdbcLoggerDataSource { // TODO?? extends AbstractLoggerSupport 

	/**
	 * optionnal configuration parameter, may be null => will use default 
	 */
	protected ConnectionFactoryConfig connectionFactoryConfig;
	
	protected String connectionFactoryConfigName = "default";
	
	protected Properties loggerProperties = new Properties();

	protected boolean logOpenClose = false;
	
	// ------------------------------------------------------------------------
	
	public AbstractJdbcLoggerDataSource() {
		super();
	}

	/** init() for underlying object lifecycle */
	public void init() {
		CommonDataSource ds = getTargetCommonDataSource();
		if (ds != null) {
			Method initMethod;
			try {
				initMethod = ds.getClass().getMethod("init", new Class[0]);
			} catch (SecurityException e) {
				initMethod = null; // ?? .. ignore!
			} catch (NoSuchMethodException e) {
				initMethod = null; // ok .. ignore!
			}

			if (initMethod != null) {
				try {
					initMethod.invoke(ds, new Object[0]);
				} catch (IllegalArgumentException e) {
					// TODO log ... ignore, no rethrow!
				} catch (IllegalAccessException e) {
					// TODO log ... ignore, no rethrow!
				} catch (InvocationTargetException e) {
					 // TODO log ...
					Throwable cause = e.getCause();
					if (cause instanceof RuntimeException) {
						throw (RuntimeException) cause;
					} else {
						throw new RuntimeException(cause);
					}
				}
			}
		}
	}
	
	/** dispose() for underlying object lifecycle */
	public void dispose() {
		CommonDataSource ds = getTargetCommonDataSource();
		if (ds != null) {
			Method disposeMethod;
			try {
				disposeMethod = ds.getClass().getMethod("dispose", new Class[0]);
			} catch (SecurityException e) {
				disposeMethod = null; // ?? .. ignore!
			} catch (NoSuchMethodException e) {
				disposeMethod = null; // ok .. ignore!
			}

			if (disposeMethod != null) {
				try {
					disposeMethod.invoke(ds, new Object[0]);
				} catch (IllegalArgumentException e) {
					// TODO log ... ignore, no rethrow!
				} catch (IllegalAccessException e) {
					// TODO log ... ignore, no rethrow!
				} catch (InvocationTargetException e) {
					 // TODO log ...
					Throwable cause = e.getCause();
					if (cause instanceof RuntimeException) {
						throw (RuntimeException) cause;
					} else {
						throw new RuntimeException(cause);
					}
				}
			}
		}
	}
	
	// ------------------------------------------------------------------------
	
	
	// ------------------------------------------------------------------------
	
	protected abstract CommonDataSource getTargetCommonDataSource();
	
	public ConnectionFactoryConfig getConnectionFactoryConfig() {
		if (connectionFactoryConfig == null) {
			connectionFactoryConfig = DriverLog.getDefaultInstance().getConnectionFactoryConfig(connectionFactoryConfigName);
		}
		return connectionFactoryConfig;
	}

	public void setConnectionFactoryConfig(ConnectionFactoryConfig p) {
		this.connectionFactoryConfig = p;
	}

	public String getConnectionFactoryConfigName() {
		return connectionFactoryConfigName;
	}

	public void setConnectionFactoryConfigName(String connectionFactoryConfigName) {
		this.connectionFactoryConfigName = connectionFactoryConfigName;
	}

	// delegate for getTargetCommonDataSource()
	// ------------------------------------------------------------------------
	

	public PrintWriter getLogWriter() throws SQLException {
		return getTargetCommonDataSource().getLogWriter();
	}

	public void setLogWriter(PrintWriter out) throws SQLException {
		getTargetCommonDataSource().setLogWriter(out);
	}
	
	public void setLoginTimeout(int seconds) throws SQLException {
		getTargetCommonDataSource().setLoginTimeout(seconds);
	}

	public int getLoginTimeout() throws SQLException {
		return getTargetCommonDataSource().getLoginTimeout();
	}
	
}
