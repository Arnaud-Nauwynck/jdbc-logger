package fr.an.tools.jdbclogger.helper.pool.dbcp;

import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.pool.PoolableObjectFactory;

import javax.sql.DataSource;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class CommonsDbcpWrapper {

	private static Class dbcpBasicDataSourceClss;
	private static Field dbcpBasicDataSource_connectionPoolField;
	private static Method dbcpBasicDataSource_createDataSource;
	private static Class dbcpGenericObjectPoolClass;
	private static Field dbcpGenericObjectPool_factoryField;
	private static Class dbcpPoolableObjectFactoryClass;
	private static Field dbcpPoolableObjectFactory_connectionFactory;
	
	static {
		try {
			dbcpBasicDataSourceClss = Class.forName("org.apache.commons.dbcp.BasicDataSource");
		} catch(Exception ex) {
			dbcpBasicDataSourceClss = null;
		}
		if (dbcpBasicDataSourceClss != null) {
			try {
				dbcpBasicDataSource_connectionPoolField = dbcpBasicDataSourceClss.getDeclaredField("connectionPool");
			} catch (SecurityException e) {
				dbcpBasicDataSource_connectionPoolField = null;
			} catch (NoSuchFieldException e) {
				dbcpBasicDataSource_connectionPoolField = null;
			}
			
			try {
				dbcpBasicDataSource_createDataSource = dbcpBasicDataSourceClss.getDeclaredMethod("createDataSource");
			} catch (SecurityException e) {
				dbcpBasicDataSource_createDataSource = null;
			} catch (NoSuchMethodException e) {
				dbcpBasicDataSource_createDataSource = null;
			}
		}
		 
		try {
			dbcpGenericObjectPoolClass = Class.forName("org.apache.commons.pool.impl.GenericObjectPool");
		} catch(Exception ex) {
			dbcpGenericObjectPoolClass = null;
		}
		if (dbcpGenericObjectPoolClass != null) {
			try {
				dbcpGenericObjectPool_factoryField = dbcpGenericObjectPoolClass.getDeclaredField("_factory");
			} catch (SecurityException e) {
				dbcpGenericObjectPool_factoryField = null;
			} catch (NoSuchFieldException e) {
				dbcpGenericObjectPool_factoryField = null;
			}
		}
		
		try {
			dbcpPoolableObjectFactoryClass = Class.forName("org.apache.commons.dbcp.PoolableConnectionFactory");
		} catch(Exception ex) {
			dbcpPoolableObjectFactoryClass = null;
		}
		if (dbcpPoolableObjectFactoryClass != null) {
			try {
				dbcpPoolableObjectFactory_connectionFactory = dbcpPoolableObjectFactoryClass.getDeclaredField("_connFactory");
			} catch (SecurityException e) {
				dbcpPoolableObjectFactory_connectionFactory = null;
			} catch (NoSuchFieldException e) {
				dbcpPoolableObjectFactory_connectionFactory = null;
			}
		}
		
	}
	
	
	public static DataSource wrap(DataSource ds, ConnectionFactoryConfig connectionFactoryConfig) {
		if (dbcpBasicDataSourceClss.isAssignableFrom(ds.getClass())) {
			BasicDataSource ds2 = (BasicDataSource) ds;
			boolean fieldAcc = dbcpBasicDataSource_connectionPoolField.isAccessible();
			if (!fieldAcc) {
				dbcpBasicDataSource_connectionPoolField.setAccessible(true);
			}
			try {
				Object/*org.apache.commons.pool.impl.GenericObjectPool*/ connectionPoolObj;
				try {
					connectionPoolObj = dbcpBasicDataSource_connectionPoolField.get(ds);
				} catch (IllegalArgumentException e) {
					connectionPoolObj = null;
				} catch (IllegalAccessException e) {
					connectionPoolObj = null;
				}
				if (connectionPoolObj == null) {
					// Field is lazy initialized in commons-dbcp => force init, then re-wrap !!!
//					Connection conn = null;
//					try {
//						conn = ds2.getConnection();
//					} catch (SQLException e1) {
//					} finally {
//						if (conn != null) {
//							try {
//								conn.close();
//							} catch (SQLException e) {
//							} 
//						}
//					}
					
					
					// ds.createDataSource();
					int prevInitSize = ds2.getInitialSize();
					ds2.setInitialSize(0);
					dbcpBasicDataSource_createDataSource.setAccessible(true);
					try {
						dbcpBasicDataSource_createDataSource.invoke(ds, new Object[0]);
					} catch(Exception ex) {
						// 
					}
					
					try {
						connectionPoolObj = dbcpBasicDataSource_connectionPoolField.get(ds);
					} catch (IllegalArgumentException e) {
						connectionPoolObj = null;
					} catch (IllegalAccessException e) {
						connectionPoolObj = null;
					}

					if (connectionPoolObj != null) {
						wrapConnectionPool(connectionPoolObj, connectionFactoryConfig);
					}
					
					ds2.setInitialSize(prevInitSize);
					
				}
				
			} finally {
				if (!fieldAcc) {
					dbcpBasicDataSource_connectionPoolField.setAccessible(fieldAcc);
				}
			}
		}
		
		return ds;
	}

	protected static void wrapConnectionPool(Object connectionPoolObj, ConnectionFactoryConfig connectionFactoryConfig) {
		Object factoryObj;
		boolean isAcc = dbcpGenericObjectPool_factoryField.isAccessible();
		try {
			if (!isAcc) {
				dbcpGenericObjectPool_factoryField.setAccessible(true);	
			}
			try {
				factoryObj = dbcpGenericObjectPool_factoryField.get(connectionPoolObj);
			} catch (IllegalArgumentException e) {
				factoryObj = null;
			} catch (IllegalAccessException e) {
				factoryObj = null;
			}
	
			if (factoryObj != null) {
				PoolableObjectFactory factoryObj2 = (PoolableObjectFactory) factoryObj;
				
				wrapPoolableObjectFactory(factoryObj2, connectionFactoryConfig);
				
			}
		} finally {
			if (!isAcc) {
				dbcpGenericObjectPool_factoryField.setAccessible(isAcc);	
			}
		}
	}

	private static void wrapPoolableObjectFactory(PoolableObjectFactory poolableFactory, ConnectionFactoryConfig connectionFactoryConfig) {
		boolean isAcc = dbcpPoolableObjectFactory_connectionFactory.isAccessible();
		try {
			if (!isAcc) {
				dbcpPoolableObjectFactory_connectionFactory.setAccessible(true);	
			}
			Object connFactory;
			try {
				connFactory = dbcpPoolableObjectFactory_connectionFactory.get(poolableFactory);
			} catch (IllegalArgumentException e) {
				connFactory = null;
			} catch (IllegalAccessException e) {
				connFactory = null;
			}
			
			if (connFactory != null) {
				ConnectionFactory connFactory2 = (ConnectionFactory) connFactory;
				// transform ConnectionFactory => new JdbcLoggerDbcpConnectionFactory(ConnectionFactory) 
				JdbcLoggerDbcpConnectionFactory wrapConnFactory = new JdbcLoggerDbcpConnectionFactory(connFactory2, connectionFactoryConfig);
				
				try {
					dbcpPoolableObjectFactory_connectionFactory.set(poolableFactory, wrapConnFactory);
				} catch (IllegalArgumentException e) {
				} catch (IllegalAccessException e) {
				}

			}
			
		} finally {
			if (!isAcc) {
				dbcpPoolableObjectFactory_connectionFactory.setAccessible(isAcc);	
			}
		}
	}
	
}
