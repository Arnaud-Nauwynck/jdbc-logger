package fr.an.tools.jdbclogger.client;

import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;

import javax.sql.CommonDataSource;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 *
 */
public class JdbcLoggerProxyDataSource extends AbstractJdbcLoggerDataSource implements DataSource {

	protected DataSource target;

	// ------------------------------------------------------------------------
	
	public JdbcLoggerProxyDataSource() {
	}
	
	public JdbcLoggerProxyDataSource(DataSource target) {
		this.target = target;
	}

	// ------------------------------------------------------------------------
	
	public DataSource getTarget() {
		return target;
	}

	public void setTarget(DataSource target) {
		this.target = target;
	}
	
	
	// ------------------------------------------------------------------------

	@Override
	protected CommonDataSource getTargetCommonDataSource() {
		return getTarget();
	}

	// implements DataSource
	// ------------------------------------------------------------------------
	
	@Override
	public Connection getConnection() throws SQLException {
		ConnectionFactoryConfig cfg = getConnectionFactoryConfig();
		String msg = "open connection from JdbcLoggerDataSource";
		Connection targetConn = getTarget().getConnection();
		Connection res = cfg.wrapOpenConn(msg, targetConn, loggerProperties, logOpenClose);
		return res;
	}

	@Override
	public Connection getConnection(String username, String password) throws SQLException {
        ConnectionFactoryConfig cfg = getConnectionFactoryConfig();
        String msg = "open connection from JdbcLoggerDataSource";
	    Connection targetConn = getTarget().getConnection(username, password);
	    Connection res = cfg.wrapOpenConn(msg, targetConn, loggerProperties, logOpenClose);
        return res;
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return getTarget().unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return getTarget().isWrapperFor(iface);
	}

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return getTarget().getParentLogger();
    }
	
}
