package fr.an.tools.jdbclogger.conf;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.XAConnection;

public class ConnectionUnwrapUtil {

    /** private to force all static */
    private ConnectionUnwrapUtil() {
    }

    // ------------------------------------------------------------------------
	
	public static Connection getUnderlyingConn(XAConnection xacon) {
		Connection con;
		try {
			con = xacon.getConnection();
		} catch (SQLException e) {
			return null;
		}
		Connection unwrappedCon = getUnderlyingConn(con);
        return unwrappedCon;
	}

	
    public static Connection getUnderlyingConn(Connection conn) {
        // handle recursively WLConnection / jdbc-logger Connection / ...
        Connection res;
        if (conn == null) {
        	return null;
        }
        Class<?> connClass = conn.getClass();
        ConnectionClassInfo conClassInfo = getConnectionClassInfo(connClass);
        if (conClassInfo.underlyingConnectionMethod != null) {
            res = ConnectionClassInfo.invokeGetConnection(conClassInfo.underlyingConnectionMethod, conn);
        } else if (conClassInfo.vendorConnectionMethod != null) {
            res = ConnectionClassInfo.invokeGetConnection(conClassInfo.vendorConnectionMethod, conn);
        } else if (conClassInfo.targetConnectionMethod != null) {
            res = ConnectionClassInfo.invokeGetConnection(conClassInfo.targetConnectionMethod, conn);
        } else if (conClassInfo.getDelegateMethod != null) {
            res = ConnectionClassInfo.invokeGetConnection(conClassInfo.getDelegateMethod, conn);
            if (res == null) {
            	// may occurs for example in common-dbcp : cf PoolGuardConnectionWrapper.setAccessToUnderlyingConnectionAllowed(false) !!!
            	if (conClassInfo.isAccessToUnderlyingConnectionAllowedMethod != null && conClassInfo.setAccessToUnderlyingConnectionAllowedMethod != null) {
            		Boolean oldAccess = (Boolean) ConnectionClassInfo.invokeMethod(conClassInfo.isAccessToUnderlyingConnectionAllowedMethod, conn, new Object[0]);
            		if (oldAccess != null && !oldAccess.booleanValue()) {
	            		ConnectionClassInfo.invokeMethod(conClassInfo.setAccessToUnderlyingConnectionAllowedMethod, conn, new Object[] { Boolean.TRUE });
	            		res = ConnectionClassInfo.invokeGetConnection(conClassInfo.getDelegateMethod, conn);
	            		ConnectionClassInfo.invokeMethod(conClassInfo.setAccessToUnderlyingConnectionAllowedMethod, conn, new Object[] { oldAccess });
            		}
            	}
            	if (res == null) {
            		res = conn; // !!
            	}
            	
            }
        } else if (conClassInfo.conField != null) {
            res = ConnectionClassInfo.invokeGetField(conClassInfo.conField, conn);
        } else {
        	res = safeUnwrapForInterface(conn, Connection.class);
        }
        if (res != conn) {
        	// *** recurse ***
        	res = getUnderlyingConn(res);
        	if (res == null) {
        		res = conn; // ??!!
        	}
        }
        return res;
    }

	public static Connection safeUnwrapForInterface(Connection con, Class<? extends Connection> connClass) {
    	Connection res = con;
        try {
			if (con.isWrapperFor(connClass)) {
				res = (Connection) con.unwrap(connClass);
				if (res == null) {
					res = con; // ??
				}
			}
		} catch (SQLException e) {
			// ignore .. no rethrow!
		} catch(java.lang.AbstractMethodError e) {
			// poor old jdbc4 driver... 
		}
        return res;
	}
	
    // internal
    // ------------------------------------------------------------------------

    /**
     *
     */
    private static final Map<Class<?>,ConnectionClassInfo> connClassInfoCache = new HashMap<>();

    private static ConnectionClassInfo getConnectionClassInfo(Class<?> clss) {
        ConnectionClassInfo cinfo = connClassInfoCache.get(clss);
        if (cinfo == null) {
            cinfo = new ConnectionClassInfo();
            
            cinfo.underlyingConnectionMethod = getPublicMethod(clss, "getUnderlyingConnection", new Class[0]);
            cinfo.vendorConnectionMethod = getPublicMethod(clss, "getVendorConnection", new Class[0]);
            cinfo.targetConnectionMethod = getPublicMethod(clss, "getTargetConnection", new Class[0]);
            cinfo.getDelegateMethod = getPublicMethod(clss, "getDelegate", new Class[0]);
            cinfo.isAccessToUnderlyingConnectionAllowedMethod = getPublicMethod(clss, "isAccessToUnderlyingConnectionAllowed", new Class[0] );
            cinfo.setAccessToUnderlyingConnectionAllowedMethod = getPublicMethod(clss, "setAccessToUnderlyingConnectionAllowed", new Class[] { boolean.class } );
            
            try {
                Field f = clss.getField("con");
                if (f != null && Connection.class.isAssignableFrom(f.getType()) 
                		&& Modifier.isPublic(f.getModifiers())
                		&& !Modifier.isStatic(f.getModifiers())
                		// && f.isAccessible()
                ) {
                    cinfo.conField = f;
                }
            } catch (Exception ex) {
                // ignore, no rethrow
            }

            connClassInfoCache.put(clss, cinfo);
        }
        return cinfo;
    }

    protected static Method getPublicMethod(Class<?> clss, String methodName, Class<?>[] sig) {
    	Method res = null;
    	if (Modifier.isPublic(clss.getModifiers())) {
	    	try {
	            res = clss.getMethod(methodName, sig);
	        } catch (Exception ex) {
	            // ignore, no rethrow
	        }
	    	if (res != null) {
	    		if (Modifier.isStatic(res.getModifiers())
	    				|| !Modifier.isPublic(res.getModifiers())) {
	    			res = null;
	    		}
	    	}
    	}
    	
    	if (res == null) {
    		// also find in super class + interfaces...
    		Class<?> superClss = clss.getSuperclass(); 
    		if (superClss != null && superClss != Object.class) {
    			res = getPublicMethod(superClss, methodName, sig);
    		}
    		if (res == null) {
    			Class<?>[] interfaces = clss.getInterfaces();
    			if (interfaces != null && interfaces.length != 0) {
    				for (Class<?> intf : interfaces) {
    					res = getPublicMethod(intf, methodName, sig);
    					if (res != null) {
    						break;
    					}
    				}
    			}
    		}
    	}
    	return res;
    }
    
    /**
     * internal class, for caching info per Connection class
     */
    private static class ConnectionClassInfo {
        Method underlyingConnectionMethod;
        Method vendorConnectionMethod;
        Method targetConnectionMethod;
        Method getDelegateMethod;
        Method isAccessToUnderlyingConnectionAllowedMethod;
        Method setAccessToUnderlyingConnectionAllowedMethod;
        
        Field conField;

        public static Connection invokeGetConnection(Method m, Connection conn) {
            Connection res;
            try {
                res = (Connection) m.invoke(conn, new Object[0]);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to get underlying vendor connection", ex);
            }
            return res;
        }

        public static Connection invokeGetField(Field f, Connection conn) {
            Connection res;
            try {
                res = (Connection) f.get(conn);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to get underlying vendor connection", ex);
            }
            return res;
        }

        public static Object invokeMethod(Method m, Connection conn, Object[] args) {
            Object res;
            try {
                res = m.invoke(conn, args);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to invoke connection method", ex);
            }
            return res;
        }

    }
	
}
