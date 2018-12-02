package fr.an.tools.jdbclogger.client;

import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;

import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 *
 */
public class JdbcLoggerProxyMixedXADataSource extends AbstractJdbcLoggerDataSource implements DataSource, XADataSource, JdbcLoggerProxyXADataSourceMBean {

	protected XADataSource target;

	protected boolean logOpenClose = false;
	
	// ------------------------------------------------------------------------
	
	public JdbcLoggerProxyMixedXADataSource(XADataSource target) {
		super();
		this.target = target;
	}

	public JdbcLoggerProxyMixedXADataSource() {
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
	
	// implements DataSource
    // ------------------------------------------------------------------------
    
    protected DataSource getTargetDS() {
        XADataSource tmp = getTarget();
        return (DataSource) tmp;
    }
    
    @Override
    public Connection getConnection() throws SQLException {
        ConnectionFactoryConfig cfg = getConnectionFactoryConfig();
        String msg = "open connection from JdbcLoggerDataSource";
        Connection targetConn = getTargetDS().getConnection();
        Connection res = cfg.wrapOpenConn(msg, targetConn, loggerProperties, logOpenClose);
        return res;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        ConnectionFactoryConfig cfg = getConnectionFactoryConfig();
        String msg = "open connection from JdbcLoggerDataSource";
        Connection targetConn = getTargetDS().getConnection(username, password);
        Connection res = cfg.wrapOpenConn(msg, targetConn, loggerProperties, logOpenClose);
        return res;
    }
    
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return getTargetDS().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return getTargetDS().isWrapperFor(iface);
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
