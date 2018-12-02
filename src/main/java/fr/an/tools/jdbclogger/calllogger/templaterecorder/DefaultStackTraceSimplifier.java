package fr.an.tools.jdbclogger.calllogger.templaterecorder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * default StackTraceSimplifier implementation, using ignored filter list and stop filter list
 */
public class DefaultStackTraceSimplifier implements StackTraceSimplifier {

    public static final StackTraceSimplifier DEFAULT_FILTER = new DefaultStackTraceSimplifier(StackElementFilter.DEFAULT_IGNORED_STACKELEMENT_FILTER_LIST,
                                                                                                Collections.emptyList());

    public static final StackTraceSimplifier DEFAULT_STOP_FILTER = new DefaultStackTraceSimplifier(StackElementFilter.DEFAULT_IGNORED_STACKELEMENT_FILTER_LIST,
                                                                                                StackElementFilter.DEFAULT_STOPPER_STACKELEMENT_FILTER_LIST);

    // -------------------------------------------------------------------------

    private List<StackElementFilter> ignoreStackElementFilterList;

    private List<StackElementFilter> stopStackElementFilterList;

    private int stopStackMoreCount = 4; 
    
    //-------------------------------------------------------------------------

    /** full ctor */
    public DefaultStackTraceSimplifier(List<StackElementFilter> ignoreStackElementFilterList,
                                       List<StackElementFilter> stopStackElementFilterList) {
        this.ignoreStackElementFilterList = new ArrayList<StackElementFilter>(ignoreStackElementFilterList);
        this.stopStackElementFilterList = new ArrayList<StackElementFilter>(stopStackElementFilterList);
    }

    /** helper ctor 
     * see StackElementFilter.createListFromRegexpsTokenizers
     */
    public static DefaultStackTraceSimplifier createFromRegexpListsTokenizer(String ignoreStackElementFilterListTokens,
                                                                             String stopStackElementFilterListTokens,
                                                                             String listElementDelimiter,
                                                                             String innerPatternDelimiter) {
        List<StackElementFilter> ignoreList = StackElementFilter.createListFromRegexpsTokenizers(
                                                                                                     ignoreStackElementFilterListTokens,
                                                                                                     listElementDelimiter,
                                                                                                     innerPatternDelimiter);
        List<StackElementFilter> stopList = StackElementFilter.createListFromRegexpsTokenizers(
                                                                                                   stopStackElementFilterListTokens,
                                                                                                   listElementDelimiter,
                                                                                                   innerPatternDelimiter);
        return new DefaultStackTraceSimplifier(ignoreList, stopList);
    }

    //-------------------------------------------------------------------------

    public StackTraceElement[] simplifyStackTrace(StackTraceElement[] stack) {
        int size = stack.length;
        List<StackTraceElement> tmpRes = new ArrayList<StackTraceElement>(size);
        for (int i = 0; i < size; i++) {
            StackTraceElement stackElt = stack[i];
            if (StackElementFilter.acceptOneOf(ignoreStackElementFilterList, stackElt)) {
                // this stack element is filtered => remove from simplified result stack, but continue
                continue;
            }
            
            if (StackElementFilter.acceptOneOf(stopStackElementFilterList, stackElt)) {
                // this stack element is a stop marker => add in simplified result stack and stop  
                tmpRes.add(stackElt);
                
                if (stopStackMoreCount > 0) {
                    // continue filtering some more, then... break
                    int morei = stopStackMoreCount;
                    for (; i < size; i++) {
                        stackElt = stack[i];
                        if (StackElementFilter.acceptOneOf(ignoreStackElementFilterList, stackElt)) {
                            continue;
                        }
                        tmpRes.add(stackElt);
                        morei--;
                        if (morei <= 0) {
                            break;
                        }
                    }
                }
                break;
            }

            tmpRes.add(stackElt);
        }
        StackTraceElement[] res = (StackTraceElement[]) tmpRes.toArray(new StackTraceElement[tmpRes.size()]);
        return res;
    }

}
