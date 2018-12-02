package fr.an.tools.jdbclogger.client;

import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;

import javax.sql.DataSource;
import javax.sql.XADataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * this class implements both DataSource and XADataSource (??) and is configurable as a bean
 */
public class JdbcLoggerBeanMixedXADataSource extends JdbcLoggerBeanXADataSource implements DataSource {
	
	// ------------------------------------------------------------------------

	public JdbcLoggerBeanMixedXADataSource() {
	}

	// ------------------------------------------------------------------------
	
	
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
    
}
