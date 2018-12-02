package fr.an.tools.jdbclogger.calllogger.templaterecorder;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
public class TemplateSqlEntry {

    /**
     * key = sql template ... see SqlUtil.templateizeSQL();
     */
    private final String sqlTemplateKey;

    private final Stats stats = new Stats();
    private final Stats fetchStats = new Stats();

    private final SampleSqlValueAndStack firstSeenInfo = new SampleSqlValueAndStack();

    private Map<String,SqlEntry> entryMap = new HashMap<>();

    private final SampleSqlValueAndStack maxReachedInfo = new SampleSqlValueAndStack();

    // ------------------------------------------------------------------------

    public TemplateSqlEntry(String sqlTemplateKey) {
        this.sqlTemplateKey = sqlTemplateKey;
    }

    // ------------------------------------------------------------------------

    public String getSqlTemplateKey() {
        return sqlTemplateKey;
    }

    public Stats getStats() {
        return stats;
    }
    
    public Stats getFetchStats() {
		return fetchStats;
	}

	public SampleSqlValueAndStack getFirstSeenInfo() {
        return firstSeenInfo;
    }
    
    public SampleSqlValueAndStack getMaxReachedInfo() {
		return maxReachedInfo;
	}

	public Map<String,SqlEntry> getEntryMapCopy() {
        Map<String,SqlEntry> res = new HashMap<String,SqlEntry>();
        synchronized (entryMap) {
            res.putAll(entryMap);
        }
        return res;
    }

    public int getSqlEntriesCount() {
        int res;
        synchronized (entryMap) {
            res = entryMap.size();
        }
        return res;
    }

    public SqlEntry getOrCreateEntry(String childSqlKey) {
        SqlEntry res;
        synchronized (entryMap) {
            res = entryMap.get(childSqlKey);
            if (res == null) {
                res = new SqlEntry(this, childSqlKey);
                entryMap.put(childSqlKey, res);
            }
        }
        return res;
    }

    public void clearEntryStats() {
        synchronized (entryMap) {
            for (SqlEntry elt : entryMap.values()) {
                elt.getStats().clear();
                elt.getStats().incr(0); // dummy incr to avoid logging "new" entries
            }
        }
    }

    public void writeDump(PrintWriter out) {
        out.println("> TemplateSqlEntry key=" + sqlTemplateKey + "\n" + "stats: " + stats + "\n" + firstSeenInfo + "\n");

        // dump child SqlEntry for template
        Map<String,SqlEntry> entryMapCopy = getEntryMapCopy();
        int size = entryMapCopy.size();
        out.println("childSqlEntriesSize = " + size + " elt(s)");
        int index = 0;
        for (SqlEntry elt : entryMap.values()) {
            out.println("> element SqlEntry [" + index + "]\n");
            elt.writeDump(out);
            out.println("< element SqlEntry\n");
        }

        out.println("< TemplateSqlEntry");
    }

}
