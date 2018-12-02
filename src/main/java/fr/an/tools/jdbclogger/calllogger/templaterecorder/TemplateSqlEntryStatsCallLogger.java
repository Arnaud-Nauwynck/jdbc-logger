package fr.an.tools.jdbclogger.calllogger.templaterecorder;

import fr.an.tools.jdbclogger.calllogger.CallMsgInfo;
import fr.an.tools.jdbclogger.calllogger.CallMsgInfoLogger;
import fr.an.tools.jdbclogger.conf.ConnectionFactoryConfig;
import fr.an.tools.jdbclogger.util.ExUtil;
import fr.an.tools.jdbclogger.util.logadapter.AbstractLoggerAdapter;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 
 */
public class TemplateSqlEntryStatsCallLogger implements CallMsgInfoLogger, TemplateSqlEntryStatsCallLoggerMBean {

    private static final String DEFAULT_DUMPSQLSTAT_FILENAME = "dumpSqlStats.csv";

    private final ConnectionFactoryConfig owner;

    private final AbstractLoggerAdapter log;

    /**
     * optional, to simplify StackTraceElement[] to keep entries
     */
    private StackTraceSimplifier stackTraceSimplifier = DefaultStackTraceSimplifier.DEFAULT_FILTER;


    /**
     * parameter to enable/disable logs on log4j category "dbstack.sqlEntries"
     * see also owner.getCurrentActiveLogs() to fully disable all logs 
     */
    private boolean activeSqlEntryLogs = true;

    /**
     * parameter
     */
    private int maxEntryPerEntryTemplate = 10;

    
    private Map<String, TemplateSqlEntry> templateSqlEntryMap = new HashMap<String, TemplateSqlEntry>();
    
    
    private int templateSqlEntryMaxElements = 5000;

    private TemplateSqlEntry defaultTemplateSqlEntryWhenTooMany = null;

    /**
     * parameter to enable/disable logs on log4j category "dbstack.sqlTemplates"
     * see also owner.getCurrentActiveLogs() to fully disable all logs 
     */
    private boolean activeTemplateSqlEntryLogs = true;

    /**
     * target logger TemplateSqlEntry
     */
    protected AbstractLoggerAdapter templateSqlEntryLogger;

    /**
     * target logger TemplateSqlEntry
     */
    protected AbstractLoggerAdapter maxReachedSqlEntryLogger;

    /**
     * target logger for SqlEntry
     */
    protected AbstractLoggerAdapter sqlEntryLogger;

    protected String preloadTemplateSqlEntryFileName;
    
    // -------------------------------------------------------------------------

    public TemplateSqlEntryStatsCallLogger(ConnectionFactoryConfig owner, Properties loggerProperties) {
        this.owner = owner;
        this.log = owner.getLogger(getClass());
        
        // TODO use properties to configure templateSqlEntryLogger name?
        String templateSqlEntryLoggerName = "dbstack.sqlTemplates";
        this.templateSqlEntryLogger = owner.getLogger(templateSqlEntryLoggerName);
        String activeTemplateSqlEntryLogsProp = loggerProperties.getProperty("activeTemplateSqlEntryLogs");
        if (activeTemplateSqlEntryLogsProp != null) {
            this.activeTemplateSqlEntryLogs = Boolean.valueOf(activeTemplateSqlEntryLogsProp).booleanValue();
        }

        maxReachedSqlEntryLogger = owner.getLogger("dbstack.maxreached");
        
        // TODO use properties to configure sqlEntryLogger name?
        String sqlEntryLoggerName = "dbstack.sqlEntries";
        this.sqlEntryLogger = owner.getLogger(sqlEntryLoggerName);
        String activeSqlEntryLogsProp = loggerProperties.getProperty("activeSqlEntryLogs");
        if (activeSqlEntryLogsProp != null) {
            this.activeSqlEntryLogs = Boolean.valueOf(activeSqlEntryLogsProp).booleanValue();
        }

        // TODO use properties to configure stackTraceSimplifier....

        // use properties to preload templateSqlEntry / sqlEntry
        this.preloadTemplateSqlEntryFileName = loggerProperties.getProperty("preloadTemplateSqlEntryFileName",
                                                                            "jdbclogger-preloadTemplateSqlEntry.txt");
        if (preloadTemplateSqlEntryFileName != null) {
            tryLoadTemplateSqlEntryFile(preloadTemplateSqlEntryFileName);
        }

    }

    // -------------------------------------------------------------------------

    @Override
    public void log(String msg) {
        // do nothing        
    }

    @Override
    public void logPendingCall(long minWaitingMillis) {
        // do nothing        
    }

    @Override
    public void logPost(CallMsgInfo stmt) {
        final String sql = stmt.fmtMsgBindVar();
        int millis = stmt.getMillis();
        
        // step 1: lookup/create TemplateSqlEntry
        TemplateSqlEntry templateEntry = getOrCreateTemplateSqlEntry(sql);
        
        stmt.setTemplateEntry(templateEntry);
    	
        String sqlTemplate = templateEntry.getSqlTemplateKey();
        Stats templateExecuteStats = templateEntry.getStats();
        // Stats templateFetchStats = templateEntry.getFetchStats();
		synchronized (templateEntry) {
            if (templateExecuteStats.getCount() == 0) {
            	// First seen info
            	SampleSqlValueAndStack info = templateEntry.getFirstSeenInfo();
            	fillSeenInfo(info, stmt);

                if (owner.isCurrentActiveLogs() && activeTemplateSqlEntryLogs
                                && templateSqlEntryLogger.isInfoEnabled()) {
                	templateSqlEntryLogger.info("found new sql template: "
                                                + "display sql with inlined values="
                                                + info.getSqlWithValues() + "\n"
                                                + ((!sql.equals(info.getSqlWithValues())) ? "\t(prepared)statement sql=" + sql + "\n" : "")
                                                + ((!sqlTemplate.equals(sql)) ? "\ttemplatized key=" + sqlTemplate + "\n" : "") 
                                                + "\tstack=" + ExUtil.stackTraceToShortPath(info.getStack()));
                }
            }

            
            templateExecuteStats.incr(millis);
            
            SampleSqlValueAndStack maxReachedInfo = templateEntry.getMaxReachedInfo();
            if (millis > maxReachedInfo.getMillis()) {
            	int prevMax = maxReachedInfo.getMillis();
            	// reached a new max value
            	fillSeenInfo(maxReachedInfo, stmt);
            	
            	if (prevMax != 0 && maxReachedSqlEntryLogger.isInfoEnabled()) {
	            	String msg = "max time reached for sqlTemplate: " + millis + " ms, " + ", prevmax:" + prevMax
	            		+ "\t template:" + sqlTemplate + "\n" 
	            		+ "\t sql with inlined values=" + maxReachedInfo.getSqlWithValues() + "\n"
	            		+ "\t stack=" + ExUtil.stackTraceToShortPath(maxReachedInfo.getStack());
	            	maxReachedSqlEntryLogger.info(msg);
            	}
            }
            
        } // synchronized(templateEntry)

        if (maxEntryPerEntryTemplate != 0 
        		&& templateEntry.getSqlEntriesCount() < maxEntryPerEntryTemplate
        		) {
	        // step2: lookup/create child SqlEntry in TemplateSqlEntry
	        SqlEntry entry = templateEntry.getOrCreateEntry(sql);
	        // synchronized(entry) {
	        if (entry.getStats().getCount() == 0) {
	            // String displaySqlWithValues = stmt.getMsgPost();
            	SampleSqlValueAndStack info = entry.getFirstSeenInfo();
            	fillSeenInfo(info, stmt);
	
	            if (owner.isCurrentActiveLogs() && activeSqlEntryLogs
	                            && sqlEntryLogger.isInfoEnabled()) {
	                sqlEntryLogger.info("new sql statement instance for template : count for template key="
	                                    + templateExecuteStats.getCount()
	                                    + " display sql with inlined values="
	                                    + info.getSqlWithValues() + "\n"
	                                    + ((!sql.equals(info.getSqlWithValues())) ? "(prepared)statement sql=" + sql + "\n" : "")
	                                    + ((!sqlTemplate.equals(sql)) ? "templatized key=" + sqlTemplate + "\n" : "")
	                                    + "stack=" + ExUtil.stackTraceToShortPath(info.getStack()));
	            }
	        }
	
	        entry.getStats().incr(stmt.getMillis());
        }
        
    }
    
    private void fillSeenInfo(SampleSqlValueAndStack info, CallMsgInfo stmt) {
    	String sql = stmt.fmtMsgBindVar();
    	String displaySqlWithValues = stmt.getArgWithValue();
        StackTraceElement[] fullstack = new Throwable().getStackTrace();
        StackTraceElement[] stack = DefaultStackTraceSimplifier.DEFAULT_FILTER.simplifyStackTrace(fullstack);
        info.setInfos(stack, displaySqlWithValues, sql, stmt.getMillis());
    }

    /** lookup/create TemplateSqlEntry */
    private TemplateSqlEntry getOrCreateTemplateSqlEntry(String sql) {
        TemplateSqlEntry res;
        synchronized (templateSqlEntryMap) {
        	// (optim) first try with exact prepared statement..
        	res = templateSqlEntryMap.get(sql);
            if (res == null) {
            	// then try with template
                String sqlTemplate = TemplateSqlUtil.templatizeSqlText(sql); 
                res = templateSqlEntryMap.get(sqlTemplate);
                if (res == null) {
                	if (templateSqlEntryMap.size() < templateSqlEntryMaxElements) {
                    	res = new TemplateSqlEntry(sqlTemplate);
    	                templateSqlEntryMap.put(sqlTemplate, res);
                	} else {
                		if (defaultTemplateSqlEntryWhenTooMany == null) {
                			defaultTemplateSqlEntryWhenTooMany = new TemplateSqlEntry("defaultTemplateSqlEntry");
                		}
                		res = defaultTemplateSqlEntryWhenTooMany;
                	}
                }
            }
        }
        return res;
    }

    @Override
    public void logPostEx(CallMsgInfo stmt) {
        // do nothing        
    }

    @Override
    public void logPre(CallMsgInfo stmt) {
        // do nothing        
    }

    @Override
    public void postIgnoreLog(CallMsgInfo msgInfo) {
        // do nothing        
    }

    @Override
    public void preIgnoreLog(CallMsgInfo msgInfo) {
        // do nothing        
    }

    @Override
    public void logPostFetch(CallMsgInfo msgInfo) {
        TemplateSqlEntry templateEntry = msgInfo.getTemplateEntry();
        if (templateEntry != null) {
        	Stats templateFetchStats = templateEntry.getFetchStats();
        	templateFetchStats.incrs(msgInfo.getResultSetNextCount(), msgInfo.getResultSetNextTotalMillis());
        }
    }

    // internal
    // ------------------------------------------------------------------------

    protected void tryLoadTemplateSqlEntryFile(String fileName) {
        File file = new File(fileName);
        if (file.exists() && file.isFile() && file.canRead()) {
            log.info("preloadTemplateSqlEntry file " + preloadTemplateSqlEntryFileName);
            doLoadTemplateSqlEntryFile(file);
        } else {
            log.info("can not find/read preloadTemplateSqlEntry file " + fileName + " ... do nothing");
        }
    }

    /** internal */
    protected void doLoadTemplateSqlEntryFile(File file) {
        BufferedReader lineReader = null;
        try {
            lineReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(file))));
            String line = null;
            while ((line = lineReader.readLine()) != null) {
                if (line.startsWith("#"))
                    continue; // comment
                if (line.trim().length() == 0)
                    continue; // empty line

                if (line.endsWith("\\")) {
                    // do concatenate several lines!
                    line = readConcatenateMultiLine(line, lineReader);
                }

                // do allocate TemplateSqlEntry for this sql line
                String key = line; // old code: key was new TemplateSqlEntryKey(stack, sqlTemplate); 
                TemplateSqlEntry templateEntry = getOrCreateTemplateSqlEntry(key);

                templateEntry.getStats().incr(0); // dummy increment counter, to avoid logging on next usage

            }
        } catch (Exception ex) {
            log.error("Failed to preload TemplateSqlEntryFile '" + file + "' ... ignore, do nothing", ex);
        } finally {
            if (lineReader != null)
                try {
                    lineReader.close();
                } catch (Exception ex) {
                }
        }
    }

    /** internal helper to concatenate current line with remaining multi-lines input */
    protected String readConcatenateMultiLine(String line, BufferedReader lineReader) throws IOException {
        StringBuilder lineBuffer = new StringBuilder();
        String lineNoAntislash = line.substring(0, line.length() - 1);
        lineBuffer.append(lineNoAntislash + "\n");
        while ((line = lineReader.readLine()) != null) {
            if (line.endsWith("\\")) {
                // continue reading multi-line
                lineNoAntislash = line.substring(0, line.length() - 1);
                lineBuffer.append(lineNoAntislash + "\n");
            } else {
                // end of multi-line
                lineBuffer.append(line);
                break;
            }
        }
        String res = lineBuffer.toString();
        return res;
    }

    // implements TemplateSqlEntryStatsCallLogger
    // ------------------------------------------------------------------------

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public boolean isCurrentActiveTemplateSqlEntryLogs() {
        return owner.isCurrentActiveLogs() && activeTemplateSqlEntryLogs;
    }

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public boolean isActiveTemplateSqlEntryLogs() {
        return activeTemplateSqlEntryLogs;
    }

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public void setActiveTemplateSqlEntryLogs(boolean p) {
        this.activeTemplateSqlEntryLogs = p;
    }

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public boolean isCurrentActiveSqlEntryLogs() {
        return owner.isCurrentActiveLogs() && activeSqlEntryLogs;
    }

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public boolean isActiveSqlEntryLogs() {
        return activeSqlEntryLogs;
    }

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public void setActiveSqlEntryLogs(boolean p) {
        this.activeSqlEntryLogs = p;
    }

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public int getTemplateSqlEntriesCount() {
        int res;
        synchronized (templateSqlEntryMap) {
            res = templateSqlEntryMap.size();
            if (defaultTemplateSqlEntryWhenTooMany != null) {
            	res += 1;
            }
        }
        return res;
    }

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public int getSqlEntriesCount() {
        int res = 0;
        synchronized (templateSqlEntryMap) {
            for (TemplateSqlEntry templateEntry : templateSqlEntryMap.values()) {
                res += templateEntry.getSqlEntriesCount();
            }
            if (defaultTemplateSqlEntryWhenTooMany != null) {
            	res += defaultTemplateSqlEntryWhenTooMany.getSqlEntriesCount();
            }
        }
        return res;
    }

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public void clearTemplateSqlEntries() {
        log.info("clearTemplateSqlEntries");
        synchronized (templateSqlEntryMap) {
            templateSqlEntryMap.clear();
            if (defaultTemplateSqlEntryWhenTooMany != null) {
            	defaultTemplateSqlEntryWhenTooMany = null;
            }
        }
    }

	@Override
    public void clearSqlStats() {
        log.info("clearSqlStats");
        synchronized (templateSqlEntryMap) {
            for (TemplateSqlEntry templateEntry : templateSqlEntryMap.values()) {
                Stats execStats = templateEntry.getStats();
				execStats.clear();
                execStats.incr(0); // dummy increment counter to avoid logging creation of creating "new template"
                
                Stats fetchStats = templateEntry.getFetchStats();
                fetchStats.clear();
                
                templateEntry.clearEntryStats();
            }
            if (defaultTemplateSqlEntryWhenTooMany != null) {
            	Stats execStats = defaultTemplateSqlEntryWhenTooMany.getStats();
            	execStats.clear();
            	execStats.incr(0);
            	defaultTemplateSqlEntryWhenTooMany.getFetchStats().clear();
            	defaultTemplateSqlEntryWhenTooMany.clearEntryStats();
            }
        }
    }

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public void loadTemplateSqlEntriesFromFileName(String fileName) {
        log.info("loadTemplateSqlEntriesFromFileName '" + fileName + "'");
        tryLoadTemplateSqlEntryFile(fileName);
    }

    public String getDumpTemplateSqlEntries() {
        log.info("getDumpTemplateSqlEntries");
        StringWriter stringWriter = new StringWriter();
        PrintWriter out = new PrintWriter(stringWriter);
        try {
            writeDumpTemplateSqlEntries(out);
        } catch (Exception ex) {
            log.error("failed to write DumpTemplateSqlEntries", ex);
            // ignore, no rethrow!
        } finally {
            if (out != null)
                try {
                    out.flush();
                } catch (Exception ex) {
                }
        }
        String res = stringWriter.getBuffer().toString();
        return res;
    }

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public void writeFileDumpTemplateSqlEntries() {
        String fileName = "dumpTemplateSqlEntries.txt";
        File file = new File(fileName);
        log.info("writeFileDumpTemplateSqlEntries: " + file.getAbsolutePath());
        PrintWriter out = null;
        try {
            out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(file)));
            writeDumpTemplateSqlEntries(out);
        } catch (Exception ex) {
            log.error("failed to write file " + file + "'", ex);
            // ignore, no rethrow!
        } finally {
            if (out != null)
                try {
                    out.close();
                } catch (Exception ex) {
                }
        }
    }

    public void writeDumpTemplateSqlEntries(PrintWriter out) {
        Map<String,TemplateSqlEntry> templateEntryMapCopy = new HashMap<String,TemplateSqlEntry>();
        TemplateSqlEntry defaultTemplateSqlEntryWhenTooManyClone = null;
        synchronized (templateSqlEntryMap) {
            templateEntryMapCopy.putAll(templateSqlEntryMap);
            if (defaultTemplateSqlEntryWhenTooMany != null) {
            	defaultTemplateSqlEntryWhenTooManyClone = defaultTemplateSqlEntryWhenTooMany; // TODO not a real clone...
            }
        }

        int size = templateEntryMapCopy.size();
        out.println("> template sql entries =  " + size + " elt(s)");
        int index = 0;
        for (TemplateSqlEntry templateEntry : templateEntryMapCopy.values()) {
            out.println(">> template sql entry element [" + index + "] =  \n");
            templateEntry.writeDump(out);
            out.println("<< template sql entry  [" + index + "]");
            index++;
        }
        if (defaultTemplateSqlEntryWhenTooManyClone != null) {
            out.println(">> template sql entry element [" + index + "] =  \n");
            defaultTemplateSqlEntryWhenTooManyClone.writeDump(out);
            out.println("<< template sql entry  [" + index + "]");
        }
        out.println("< template sql entries =  " + size + " elt(s)");
    }

    /** implements TemplateSqlEntryStatsCallLoggerMBean */
    public String getCsvDumpSqlStats() {
        // log.info("getCsvDumpSqlStats");
        StringWriter stringWriter = new StringWriter();
        PrintWriter out = new PrintWriter(stringWriter);
        try {
            doPrintCsvFileDumpSqlStats(out);
        } catch (Exception ex) {
            log.error("failed to write CsvDumpSqlStats", ex);
            // ignore, no rethrow!
        } finally {
            if (out != null)
                try {
                    out.flush();
                } catch (Exception ex) {
                }
        }
        String res = stringWriter.getBuffer().toString();
        return res;
    }

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public String[][] getArrayDumpSqlStats() {
        // log.info("getArrayDumpSqlStats");
        List<String[]> tmpresLines = new ArrayList<>();
        List<String> currLine = new ArrayList<>();
        Collection<TemplateSqlEntry> templateEntryCopy = new ArrayList<>();
        synchronized (templateSqlEntryMap) {
            templateEntryCopy.addAll(templateSqlEntryMap.values());
        }
        // NOT IMPLEMENTED... use defaultTemplateSqlEntryWhenTooMany

        SortedSet<TemplateSqlEntry> sortedLineCount = new TreeSet<>(new TemplateSqlEntryStatsComparator());
        sortedLineCount.addAll(templateEntryCopy);

        currLine.add("templateStmtKey");
        currLine.add("preparedStmt");
        currLine.add("stmtWithValue");
        currLine.add("stack");
        currLine.add("detailedStack");

        currLine.add("totalMillis");
        currLine.add("count");
        currLine.add("avgMillis");

        currLine.add("maxMillis");
        currLine.add("maxStmtWithValue");
        currLine.add("maxStack");
        currLine.add("maxDetailedStack");

        tmpresLines.add(stringListToStringArray(currLine));
        currLine.clear();

        for (TemplateSqlEntry elt : sortedLineCount) {
            Stats stats = elt.getStats();
            SampleSqlValueAndStack firstSeenInfo = elt.getFirstSeenInfo();
            SampleSqlValueAndStack maxSeenInfo = elt.getMaxReachedInfo();

            currLine.add(elt.getSqlTemplateKey());
            currLine.add(firstSeenInfo.getSqlStmt());
            currLine.add(firstSeenInfo.getSqlWithValues());
            
            StackTraceElement[] firstSeenStack = firstSeenInfo.getStack();
            if (firstSeenStack != null) {
                StackTraceElement[] firstSeenSimplifiedStack = stackTraceSimplifier.simplifyStackTrace(firstSeenStack);
                currLine.add(ExUtil.stackTraceToShortPath(firstSeenSimplifiedStack));
                currLine.add(ExUtil.stackTraceToShortPath(firstSeenStack));            
            } else {
                currLine.add("?");
                currLine.add("?");
            }

            currLine.add(Long.toString(stats.getTotalMillis()));
            currLine.add(Integer.toString(stats.getCount()));

            double avgMillis = stats.getTotalMillis() / ((stats.getCount() != 0) ? stats.getCount() : 1);
            currLine.add(Integer.toString((int) avgMillis));

            // maxMillis
            currLine.add(Integer.toString(maxSeenInfo.getMillis()));
            // maxStmtWithValue
            currLine.add(maxSeenInfo.getSqlWithValues());
            
            // maxStack, maxDetailedStack
            StackTraceElement[] maxSeenStack = maxSeenInfo.getStack();
            if (firstSeenStack != null) {
                StackTraceElement[] maxSimplifiedStack = stackTraceSimplifier.simplifyStackTrace(maxSeenStack);
                currLine.add(ExUtil.stackTraceToShortPath(maxSimplifiedStack));
                currLine.add(ExUtil.stackTraceToShortPath(maxSeenStack));            
            } else {
                currLine.add("?");
                currLine.add("?");
            }
            
            tmpresLines.add(stringListToStringArray(currLine));
            currLine.clear();
        }

        int lineCount = tmpresLines.size();
        String[][] res = new String[lineCount][];
        tmpresLines.toArray(res);
        return res;
    }

    private static String[] stringListToStringArray(List<String> p) {
        return p.toArray(new String[p.size()]);
    }

    /** implements TemplateSqlEntryStatsCallLoggerMBean */
	@Override
    public void writeCsvFileDumpSqlStats() {
        writeCsvFileDumpSqlStats2(DEFAULT_DUMPSQLSTAT_FILENAME);
    }

    /** implements TemplateSqlEntryStatsCallLogger */
	@Override
    public void writeCsvFileDumpSqlStats2(String fileName) {
        log.info("writeCsvFileDumpSqlStats fileName=" + fileName);
        File file = new File(fileName);
        PrintWriter out = null;
        try {
            out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(file)));
            doPrintCsvFileDumpSqlStats(out);
        } catch (Exception ex) {
            log.error("failed to write file " + file + "'", ex);
            // ignore, no rethrow!
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception ex) {
                }
            }
        }
    }

    /**
     * internal comparator helper
     */
    private static class TemplateSqlEntryStatsComparator implements Comparator<TemplateSqlEntry> {

    	@Override
        public int compare(TemplateSqlEntry p1, TemplateSqlEntry p2) {
            long totalMillis1 = p1.getStats().getTotalMillis() + p1.getFetchStats().getTotalMillis();
            long totalMillis2 = p2.getStats().getTotalMillis() + p2.getFetchStats().getTotalMillis();
            int res;
            if (totalMillis1 < totalMillis2) {
                res = +1;
            } else if (totalMillis1 > totalMillis2) {
                res = -1;
            } else {
                // compare by text..
                res = p1.getSqlTemplateKey().compareTo(p2.getSqlTemplateKey());
            }
            return res;
        }

    }

    /** internal */
    protected void doPrintCsvFileDumpSqlStats(PrintWriter out) {
        Collection<TemplateSqlEntry> templateEntryCopy = new ArrayList<TemplateSqlEntry>();
        synchronized (templateSqlEntryMap) {
            templateEntryCopy.addAll(templateSqlEntryMap.values());
        }

        SortedSet<TemplateSqlEntry> sortedLineCount = new TreeSet<TemplateSqlEntry>(new TemplateSqlEntryStatsComparator());
        sortedLineCount.addAll(templateEntryCopy);

        String csvSep = ";";

        out.print("templateStmtKey" + csvSep 
        		+ "firstPStmt" + csvSep + "firstStmtWithValue" + csvSep 
        		+ "firstStack" + csvSep + "firstStackD" + csvSep 
                
        		+ "maxReachedMillis" + csvSep + "maxreachedPStmt" + csvSep + "maxreachedStmtWithValue" + csvSep
                + "maxReachedStack" + csvSep + "maxReachedStackD" + csvSep

                + "totalMillis" + csvSep + "count" + csvSep + "avgMillis" + csvSep
                + "fetchTotalMillis" + csvSep + "rowsCount" + csvSep
        	);
        out.println();

        for (TemplateSqlEntry elt : sortedLineCount) {
        	String templateStmtKey = strToCsvProtectedStr(elt.getSqlTemplateKey());
        	try {
	            Stats execStats = elt.getStats();
	            Stats fetchStats = elt.getFetchStats();
	            
	            out.print(templateStmtKey);
	            out.print(csvSep);
	
	            { SampleSqlValueAndStack firstSeenInfo = elt.getFirstSeenInfo();
	            out.print(strToCsvProtectedStr(firstSeenInfo.getSqlStmt()));
	            out.print(csvSep);
	            out.print(strToCsvProtectedStr(firstSeenInfo.getSqlWithValues()));
	            out.print(csvSep);
	            doPrintSimplifiedStackAndStack(out, csvSep, firstSeenInfo.getStack());
	            }
	            
	            // maxReachedMillis, maxreachedPStmt, maxreachedStmtWithValue, maxReachedStack, maxReachedDetailedStack
	            { SampleSqlValueAndStack maxReachedInfo = elt.getMaxReachedInfo();
	            out.print(maxReachedInfo.getMillis());
	            out.print(csvSep);
	            out.print(strToCsvProtectedStr(maxReachedInfo.getSqlStmt()));
	            out.print(csvSep);
	            out.print(strToCsvProtectedStr(maxReachedInfo.getSqlWithValues()));
	            out.print(csvSep);
	            doPrintSimplifiedStackAndStack(out, csvSep, maxReachedInfo.getStack());
	            }
	
	            long execAndFetchMillis = execStats.getTotalMillis() + fetchStats.getTotalMillis(); 
	            out.print(execAndFetchMillis);
	            out.print(csvSep);
	
	            int count = execStats.getCount();
				out.print(count);
	            out.print(csvSep);
	
	            int execAndFetchAvgMillis = (int) (execAndFetchMillis / (count != 0? count : 1));
	            out.print(execAndFetchAvgMillis);
	            out.print(csvSep);
	            
	            out.print(fetchStats.getTotalMillis());
	            out.print(csvSep);
	
	            out.print(fetchStats.getCount());
	            out.print(csvSep);
	            
        	} catch(Exception ex) {
        		// Failed to print line ..ignore, continue!!
        		log.error("Failed doPrintCsvFileDumpSqlStats on line " + templateStmtKey + " : " + ex.getMessage() + "... continue");
        	}
	           
        	out.println();
        }

    }

    private void doPrintSimplifiedStackAndStack(PrintWriter out, String csvSep, StackTraceElement[] stack) {
        if (stack != null) {
            StackTraceElement[] simplifiedStack = stackTraceSimplifier.simplifyStackTrace(stack);
            out.print(ExUtil.stackTraceToShortPath(simplifiedStack));
            out.print(csvSep);
            out.print(ExUtil.stackTraceToShortPath(stack));
            out.print(csvSep);
        } else {
            out.print("?");
            out.print(csvSep);
            out.print("?");
            out.print(csvSep);
        }
    }

    private String strToCsvProtectedStr(String p) {
    	if (p == null) {
    		return "";
    	}
        String res = p;
        res = res.replaceAll("\t", "  ");
        res = res.replaceAll("\n", "\\n");
        res = res.replaceAll("\"", "'");
        res = res.replaceAll(";", ",");
        return "\"" + res + "\"";
    }

    /** implements TemplateSqlEntryStatsCallLogger */
    public void writeFileDumpProblemBindVariables() {
        log.info("writeFileDumpProblemBindVariables");
        String fileName = "dumpProblemBindVariablesStmt.csv";
        File file = new File(fileName);
        PrintWriter out = null;
        try {
            out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(file)));
            doPrintDumpProblemBindVariables(out);
        } catch (Exception ex) {
            log.error("failed to write file " + file + "'", ex);
            // ignore, no rethrow!
        } finally {
            if (out != null)
                try {
                    out.close();
                } catch (Exception ex) {
                }
        }
    }

    /** internal */
    protected void doPrintDumpProblemBindVariables(PrintWriter out) {
        Collection<TemplateSqlEntry>templateEntryCopy = new ArrayList<>();
        synchronized (templateSqlEntryMap) {
            templateEntryCopy.addAll(templateSqlEntryMap.values());
        }

        Map<Integer,TemplateSqlEntry> tmpSortedTemplateEntries = new TreeMap<>();
        for (TemplateSqlEntry templateEntry : templateEntryCopy) {
            int count = templateEntry.getSqlEntriesCount();
            if (count > 2) {
                tmpSortedTemplateEntries.put(new Integer(-count), templateEntry); // use descendant order with "- count"     
            }
        }

        String csvSep = ";";
        out.println("TemplateSqlKey" + csvSep 
        			+ "SqlEntriesCount" + csvSep 
        			+ "firstSeenPreparedStmt" + csvSep
                    + "firstSeenStmtWithValue" + csvSep 
                    + "stack" + csvSep 
                    + "detailedStack" + csvSep 
                    + "totalMillis" + csvSep 
                    + "count" + csvSep);

        for (TemplateSqlEntry elt : tmpSortedTemplateEntries.values()) {
            Stats stats = elt.getStats();
            SampleSqlValueAndStack firstSeenInfo = elt.getFirstSeenInfo();

            out.print(strToCsvProtectedStr(elt.getSqlTemplateKey()));
            out.print(csvSep);

            out.print(elt.getSqlEntriesCount());
            out.print(csvSep);

            out.print(strToCsvProtectedStr(firstSeenInfo.getSqlStmt()));
            out.print(csvSep);

            out.print(strToCsvProtectedStr(firstSeenInfo.getSqlWithValues()));
            out.print(csvSep);

            doPrintSimplifiedStackAndStack(out, csvSep, firstSeenInfo.getStack());

            out.print(stats.getTotalMillis());
            out.print(csvSep);

            out.print(stats.getCount());
            out.print(csvSep);

            out.println();
        }
    }

}
