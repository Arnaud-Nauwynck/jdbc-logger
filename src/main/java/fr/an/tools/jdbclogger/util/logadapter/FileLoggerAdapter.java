package fr.an.tools.jdbclogger.util.logadapter;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Adtapter for direct java.io.PrintStream using File
 */
public class FileLoggerAdapter extends AbstractLoggerAdapter {

	/**
	 * Factory for FileLoggerAdapter
	 */
	public static class FileLoggerAdapterFactory extends LoggerAdapterFactory {
		
		private File logFile;
		private PrintStream logPrintStream;

		public FileLoggerAdapterFactory(String fileName) {
			if (fileName == null || fileName.equals("stdout")) {
				logPrintStream = System.out;
			} else if (fileName.equals("stderr")) {
				logPrintStream = System.err;
			} else {
				this.logFile = new File(fileName);
				if (!logFile.getParentFile().exists()) {
					logFile.getParentFile().mkdirs();
				}
			}
			// checkOpenFile();
		}
		
		@Override
		public AbstractLoggerAdapter getLogger(String name) {
			return new FileLoggerAdapter(this, name);
		}
		
		public boolean checkOpenFile() {
			if (logPrintStream == null) {
				try {
					logPrintStream = new PrintStream(logFile);
				} catch(IOException ex) {
					// Failed to open file for writting! ... ignore 
				}
			}
			return logPrintStream != null;
		}
		
		public void writeLog(String level, String categoryName, String msg) {
			checkOpenFile();
			if (logPrintStream == null) return;
			String logLine = level + " " + categoryName + " " + msg;
			logPrintStream.println(logLine);
		}

		public void writeLogEx(String level, String categoryName, String msg, Throwable ex) {
			checkOpenFile();
			if (logPrintStream == null) return;
			String logLine = level + " " + categoryName + " " + msg;
			logPrintStream.println(logLine);
			ex.printStackTrace(logPrintStream);
			logPrintStream.flush();
		}

	}
	
	private FileLoggerAdapterFactory owner;
	private String categoryName;
	
	// ------------------------------------------------------------------------
	
	protected FileLoggerAdapter(FileLoggerAdapterFactory owner, String categoryName) {
		this.owner = owner;
		this.categoryName = categoryName;
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public boolean isInfoEnabled() {
		return true;
	}

	@Override
	public void info(String msg) {
		owner.writeLog("info", categoryName, msg);
	}

	@Override
	public void warn(String msg) {
		owner.writeLog("warn", categoryName, msg);
	}
	
	@Override
	public void warn(String msg, Throwable ex) {
		owner.writeLogEx("warn", categoryName, msg, ex);
	}
	
	@Override
	public void error(String msg) {
		owner.writeLog("error", categoryName, msg);
	}
	
	@Override
	public void error(String msg, Throwable ex) {
		owner.writeLogEx("error", categoryName, msg, ex);
	}

}
