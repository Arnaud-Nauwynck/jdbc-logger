package fr.an.tools.jdbclogger.calllogger.templaterecorder;

/**
 * 
 */
public class Stats {

    private int count;
    private long totalMillis;
    
    // ------------------------------------------------------------------------

    public Stats() {
    }

    // ------------------------------------------------------------------------

    public int getCount() {
        return count;
    }

    public long getTotalMillis() {
        return totalMillis;
    }

    public double getAvgMillis() {
    	long c = count;
    	return totalMillis / (c != 0? c : 1);
    }
    
    public void incr(long millis) {
        this.count++;
        this.totalMillis += millis;
    }

    public void incrs(int incrCount, long millis) {
        this.count += incrCount;
        this.totalMillis += millis;
    }

    public void clear() {
        this.count = 0;
        this.totalMillis = 0;
    }

    // ------------------------------------------------------------------------

    public String toString() {
        return "count=" + count + ", totalMillis=" + totalMillis;
    }

}
