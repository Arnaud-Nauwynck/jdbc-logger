package fr.an.tools.jdbclogger.helper.spring;

import fr.an.tools.jdbclogger.DriverLog;
import fr.an.tools.jdbclogger.client.AbstractJdbcLoggerDataSource;
import fr.an.tools.jdbclogger.client.JdbcLoggerProxyDataSource;
import fr.an.tools.jdbclogger.client.JdbcLoggerProxyMixedXADataSource;
import fr.an.tools.jdbclogger.client.JdbcLoggerProxyXADataSource;
import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;
import fr.an.tools.jdbclogger.conf.DriverConfigLog;
import fr.an.tools.jdbclogger.helper.pool.dbcp.CommonsDbcpWrapper;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import javax.sql.DataSource;
import javax.sql.XADataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * spring helper class for wrapping DataSource and XADataSource with jdbc-logger 
 */
public class JdbcLoggerBeanPostProcessor implements BeanPostProcessor {

	private boolean enableWrapper = true;
	
	private List<String> beanNames = new ArrayList<String>();
	
	private List<Pattern> _cachedBeanNamePatterns;
	
	
	protected DriverConfigLog driverConfigLog;
	
	protected Map<String,ConnectionFactoryConfig> beanNameToConnectionFactoryConfig;
//	/**
//	 * optionnal configuration parameter, may be null => will use default 
//	 */
//	protected ConnectionFactoryConfig connectionFactoryConfig;

	
	// ------------------------------------------------------------------------

	public JdbcLoggerBeanPostProcessor() {
	}

	// implements spring BeanPostProcessor
	// ------------------------------------------------------------------------

    // simply return the instantiated bean as-is
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        if (!enableWrapper) {
        	return bean;
        }
        Object res = bean;
        if ( ((bean instanceof DataSource) || (bean instanceof XADataSource))
        		&& isEnableWrapBeanName(beanName)
        		&& !(bean instanceof AbstractJdbcLoggerDataSource)
        		) {
        	if (driverConfigLog == null) {
        		driverConfigLog = DriverLog.getDefaultInstance(); 
        	}
        	
        	ConnectionFactoryConfig connectionFactoryConfig = (beanNameToConnectionFactoryConfig != null)? beanNameToConnectionFactoryConfig.get(beanName) : null;
        	if (connectionFactoryConfig == null) {
        		String configKey = beanName; // TODO url?
        		connectionFactoryConfig = driverConfigLog.getConnectionFactoryConfig(configKey);
        	}
        	
            if (bean instanceof DataSource && bean instanceof XADataSource) {
                XADataSource ds = (XADataSource) bean;
                JdbcLoggerProxyMixedXADataSource res2 = new JdbcLoggerProxyMixedXADataSource(ds);
                res = res2;
                if (connectionFactoryConfig != null) {
                    res2.setConnectionFactoryConfig(connectionFactoryConfig);
                }
            } else if (bean instanceof DataSource) {
	    		DataSource ds = (DataSource) bean;
	    		
	    		if (ds.getClass().getName().equals("org.apache.commons.dbcp.BasicDataSource")) {
	    			res = CommonsDbcpWrapper.wrap((org.apache.commons.dbcp.BasicDataSource) ds, connectionFactoryConfig);
	    		} else {
		    		// TODO ?? ... instead of  "JdbcLoggerProxyDataSource(PoolableDataSource( ..)) => use "PoolableDataSource(ObjectFactory(LoggerProxy(...)))
		    		JdbcLoggerProxyDataSource res2 = new JdbcLoggerProxyDataSource(ds);
		    		res = res2;
		    		if (connectionFactoryConfig != null) {
		    			res2.setConnectionFactoryConfig(connectionFactoryConfig);
		    		}
	    		}
	        } else if (bean instanceof XADataSource) {
	    		XADataSource ds = (XADataSource) bean;
	    		JdbcLoggerProxyXADataSource res2 = new JdbcLoggerProxyXADataSource(ds);
				res = res2;
				if (connectionFactoryConfig != null) {
					res2.setConnectionFactoryConfig(connectionFactoryConfig);
				}
	        }
        }
    	return res;
    }

    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (!enableWrapper) {
        	return bean;
        }
        return bean;
    }
 
    // ------------------------------------------------------------------------
    
	public boolean isEnableWrapper() {
		return enableWrapper;
	}

	public void setEnableWrapper(boolean enableWrapper) {
		this.enableWrapper = enableWrapper;
	}

	public List<String> getBeanNames() {
		return beanNames;
	}

	public void setBeanNames(List<String> beanNames) {
		this.beanNames = beanNames;
	}
    
	
	public DriverConfigLog getDriverConfigLog() {
		return driverConfigLog;
	}
	
	public void setDriverConfigLog(DriverConfigLog driverConfigLog) {
		this.driverConfigLog = driverConfigLog;
	}
	
    // ------------------------------------------------------------------------

	private List<Pattern> getBeanNamePatterns() {
    	List<Pattern> res = _cachedBeanNamePatterns;
    	if (res == null) {
    		res = new ArrayList<Pattern>();
    		if (beanNames == null || beanNames.isEmpty()) {
    			res.add(Pattern.compile(".*"));
    		} else {
    			for (String beanName : beanNames) {
    				res.add(Pattern.compile(beanName));
    			}
    		}
    		_cachedBeanNamePatterns = res;
    	}
    	return res;
    }
    
    protected boolean isEnableWrapBeanName(String beanName) {
    	boolean res = false;
    	for (Pattern beanNamePattern : getBeanNamePatterns()) {
    		if (beanNamePattern.matcher(beanName).matches()) {
    			res = true;
    			break;
    		}
    	}
    	return res;
    }

}
