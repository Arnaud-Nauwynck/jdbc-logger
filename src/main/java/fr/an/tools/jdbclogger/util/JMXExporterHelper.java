package fr.an.tools.jdbclogger.util;

import fr.an.tools.jdbclogger.conf.DriverConfigLog;
import fr.an.tools.jdbclogger.util.logadapter.AbstractLoggerAdapter;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * static helper methods for JMX export
 */
public final class JMXExporterHelper {

    private static final String HTML_ADAPTOR_SERVER_CLASSNAME = "com.sun.jdmk.comm.HtmlAdaptorServer";

    
    private DriverConfigLog owner;
    private AbstractLoggerAdapter log;

    /**
     * for JMX: JNDI name of MBeanServer to export MBeans 
     */
    private String jmxMBeanServerJndiName;
    private String jmxMBeanServerJndiName2;
    private String jmxMBeanServerLocalHomeJndiName3;

    private boolean enableHtmlAdaptorServer;
    private Object createdHtmlAdaptorServer;

    private boolean mbeanServerAlreadyInitialized;
    private MBeanServer mbeanServer;

    // ------------------------------------------------------------------------

    public JMXExporterHelper(DriverConfigLog owner) {
    	this.owner = owner;
    	this.log = owner.getLogger(getClass());
    }

    public void init(Properties properties) {
        this.jmxMBeanServerJndiName = properties.getProperty("jmxMBeanServerJndiName", "weblogic.management.server");
        this.jmxMBeanServerJndiName2 = properties.getProperty("jmxMBeanServerJndiName2",
                                                              "weblogic.management.mbeanservers.runtime");
        this.jmxMBeanServerLocalHomeJndiName3 = properties.getProperty("jmxMBeanServerLocalHomeJndiName2",
                                                                       "weblogic.management.home.localhome");

        if (jmxMBeanServerJndiName != null && jmxMBeanServerJndiName.equals("null")) {
            jmxMBeanServerJndiName = null;
        }
        if (jmxMBeanServerJndiName2 != null && jmxMBeanServerJndiName2.equals("null")) {
            jmxMBeanServerJndiName2 = null;
        }
        if (jmxMBeanServerLocalHomeJndiName3 != null && jmxMBeanServerLocalHomeJndiName3.equals("null")) {
            jmxMBeanServerLocalHomeJndiName3 = null;
        }

        enableHtmlAdaptorServer = stringToBool(properties.getProperty("enableHtmlAdaptorServer", "false"));

        if (enableHtmlAdaptorServer) {
            try {
                createdHtmlAdaptorServer = createHttpAdaptor();
            } catch (Exception ex) {
                log.warn("Failed createHttpAdaptor ... ignore", ex);
            }
        }
    }

    private static boolean stringToBool(String p) {
        return p.equalsIgnoreCase("y") || p.equalsIgnoreCase("yes") || p.equalsIgnoreCase("true");
    }

    // ------------------------------------------------------------------------

    
    public Object getCreatedHtmlAdaptorServer() {
        return createdHtmlAdaptorServer;
    }

    public DriverConfigLog getOwner() {
        return owner;
    }

    public void setOwner(DriverConfigLog owner) {
        this.owner = owner;
    }

    public void setCreatedHtmlAdaptorServer(Object p) {
        this.createdHtmlAdaptorServer = p;
    }

    private Object lookupJndi(String jndiName) {
        try {
            InitialContext ic = new InitialContext();
            Object res = ic.lookup(jndiName);
            return res;
        } catch (NamingException ex) {
            throw new RuntimeException("Failed to lookup MBeanServer from Jndi name='" + jndiName + "'", ex);
        }
    }

    /**
     * @param exportMBeanServerJndiName
     * @param mbean
     * @param beanName
     */
    public void registerMBean(Object mbean, String mbeanObjectName) {
        MBeanServer mbeanServer = getMbeanServerOrNull();
        if (mbeanServer != null) {
            try {
                // step 1: execute "objectName = new ObjectName(mbeanDomainToExport + ":" + baseNameToExport)"
                Class clss = mbeanServer.getClass();
                Class objectNameClass = Class.forName("javax.management.ObjectName");
                Constructor objectNameCtor = objectNameClass.getConstructor(new Class[] { String.class });
                Object[] objectNameArgs = new Object[] { mbeanObjectName };
                Object objectName = objectNameCtor.newInstance(objectNameArgs);

                // step 2: execute "mbeanServer.registerMBean(this, objectName)"
                Method registerMethod = clss.getMethod("registerMBean", new Class[] { Object.class, objectNameClass });
                Object[] args = new Object[] { mbean, objectName };
                registerMethod.invoke(mbeanServer, args);
            } catch (Exception ex) {
                String msg = "Failed to register MBean '" + mbean + "' " + " with objectName '" + mbeanObjectName
                             + "' "
                             // + ", ex=" + ex.getMessage()
                             ;
                log.warn(msg, ex);
            }
        }
    }

    /**
     * @param exportMBeanServerJndiName
     * @param mbeanObjectName
     */
    public void unregisterMBean(String mbeanObjectName) {
        MBeanServer mbeanServer = getMbeanServerOrNull();
        if (mbeanServer != null) {
            try {
                // step 1: execute "objectName = new ObjectName(mbeanDomainToExport + ":" + baseNameToExport)"
                Class clss = mbeanServer.getClass();
                Class objectNameClass = Class.forName("javax.management.ObjectName");
                Constructor objectNameCtor = objectNameClass.getConstructor(new Class[] { String.class });
                Object[] objectNameArgs = new Object[] { mbeanObjectName };
                Object objectName = objectNameCtor.newInstance(objectNameArgs);

                // step 2: execute "mbeanServer.unregisterMBean(this, objectName)"
                Method registerMethod = clss.getMethod("unregisterMBean", new Class[] { objectNameClass });
                Object[] args = new Object[] { objectName };
                registerMethod.invoke(mbeanServer, args);

            } catch (Exception ex) {
                String msg = "Failed to unregister MBean with objectName '" + mbeanObjectName + "'" 
                			// + ", ex=" + ex.getMessage()
                			;
                log.warn(msg, ex);
            }
        }
    }

    private MBeanServer getMbeanServerOrNull() {
        if (!mbeanServerAlreadyInitialized) {
            mbeanServerAlreadyInitialized = true;

            if (mbeanServer == null) {
                try {
                    mbeanServer = doGetMBeanServer();
                } catch (Exception ex) {
                    log.warn("Failed to get MBeanServer ... use null");
                    mbeanServer = null;
                }
            }
        }
        return mbeanServer;
    }

    private MBeanServer doGetMBeanServer() {
        // test if jdk5 ==> use "MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();"
        MBeanServer res = null;
        Class managementFactoryClass = null;
        try {
            managementFactoryClass = Class.forName("java.lang.management.ManagementFactory");
        } catch (Exception ex) {
            log.info("no Jdk >=5, can not ManagementFactory.getPlatformMBeanServer()");
        }
        if (managementFactoryClass != null) {
            try {
                Method meth = managementFactoryClass.getMethod("getPlatformMBeanServer", new Class[0]);
                Object tmp = meth.invoke(null, new Object[0]);
                res = (MBeanServer) tmp;
                if (res != null) {
                    return res;
                }
            } catch (Exception ex) {
                log.warn("Failed to use ManagementFactory.getPlatformMBeanServer(), ex=" + ex.getMessage());
            }
        }

        return res;
    }

    private Object createHttpAdaptor() {
        Object res = null;

        Class htmlAdaptorServerClass = null;
        try {
            htmlAdaptorServerClass = Class.forName(HTML_ADAPTOR_SERVER_CLASSNAME);
        } catch (Exception ex) {
            log.info("class " + HTML_ADAPTOR_SERVER_CLASSNAME
                     + " not found in classpath... do not create JMX html server");
        }
        if (htmlAdaptorServerClass != null) {
            try {
                // HtmlAdaptorServer htmlAdaptorServer = new com.sun.jdmk.comm.HtmlAdaptorServer();

                res = htmlAdaptorServerClass.newInstance();

                // adapter.setPort(8005);
                Method setPortMethod = htmlAdaptorServerClass.getDeclaredMethod("setPort",
                                                                                new Class[] { Integer.class });
                setPortMethod.invoke(res, new Object[] { new Integer(8005) });

                // htmlAdaptorServer.start();
                Method startMethod = htmlAdaptorServerClass.getDeclaredMethod("start", new Class[0]);
                startMethod.invoke(res, new Object[0]);

            } catch (Exception ex) {
                log.warn("Failed to create a HtmlAdaptorServer for MX MBeanServer ... ignore:" + ex.getMessage());
            }

            if (res != null) {
                MBeanServer mbs = getMbeanServerOrNull();
                if (mbs != null) {
                    try {
                        // Register and start the HTML adaptor
                        ObjectName adapterName = new ObjectName("SimpleAgent:name=htmladapter");
                        mbs.registerMBean(res, adapterName);
                    } catch (Exception ex) {
                        log.warn("Failed to register HtmlAdaptorServer in JMX MBeanServer ... ignore:"
                                 + ex.getMessage());
                    }
                }
            }

        }
        return res;
    }

}
