package fr.an.tools.jdbclogger.client;

import javax.sql.CommonDataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 *
 */
public class JdbcLoggerProxyXADataSource extends AbstractJdbcLoggerDataSource implements XADataSource, JdbcLoggerProxyXADataSourceMBean {

	protected XADataSource target;

	protected boolean logOpenClose = false;
	
	// ------------------------------------------------------------------------
	
	public JdbcLoggerProxyXADataSource(XADataSource target) {
		super();
		this.target = target;
	}

	public JdbcLoggerProxyXADataSource() {
		super();
	}

	// ------------------------------------------------------------------------

	public XADataSource getTarget() {
		return target;
	}

	public void setTarget(XADataSource target) {
		this.target = target;
	}
	
	@Override // override AbstractJdbcLoggerDataSource
	protected CommonDataSource getTargetCommonDataSource() {
		return getTarget();
	}
	
	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
	    return getTarget().getParentLogger();
	}
	
	// implements XADataSource
	// ------------------------------------------------------------------------
	
    @Override
	public XAConnection getXAConnection() throws SQLException {
		XAConnection underlyingXACon = getTarget().getXAConnection();
		XAConnection res = getConnectionFactoryConfig().wrapOpenConn("XADataSource.getXAConnection", underlyingXACon, loggerProperties, logOpenClose);
		return res;
	}

	@Override
	public XAConnection getXAConnection(String user, String password) throws SQLException {
		XAConnection underlyingXACon = getTarget().getXAConnection(user, password);
		XAConnection res = getConnectionFactoryConfig().wrapOpenConn("XADataSource.getXAConnection", underlyingXACon, loggerProperties, logOpenClose);
		return res;
	}

}
