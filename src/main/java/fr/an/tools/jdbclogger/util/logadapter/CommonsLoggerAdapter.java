package fr.an.tools.jdbclogger.util.logadapter;


/**
 * Adapter for apache commons-logging 
 */
public class CommonsLoggerAdapter extends AbstractLoggerAdapter {

	/**
	 * Factory for CommonsLoggerAdapter
	 */
	public static class CommonsLoggerAdapterFactory extends LoggerAdapterFactory {
		
		public CommonsLoggerAdapterFactory() {
		}
		
		@Override
		public AbstractLoggerAdapter getLogger(String name) {
			return new CommonsLoggerAdapter(name);
		}
	}
	
	private org.apache.commons.logging.Log log;
	
	// ------------------------------------------------------------------------
	
	protected CommonsLoggerAdapter(String name) {
		log = org.apache.commons.logging.LogFactory.getLog(name);
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public boolean isInfoEnabled() {
		return log.isInfoEnabled();
	}

	@Override
	public void info(String msg) {
		log.info(msg);
	}

	@Override
	public void warn(String msg) {
		log.warn(msg);
	}
	
	@Override
	public void warn(String msg, Throwable ex) {
		log.warn(msg, ex);
	}

	@Override
	public void error(String msg) {
		log.error(msg);
	}
	
	@Override
	public void error(String msg, Throwable ex) {
		log.error(msg, ex);
	}

}
