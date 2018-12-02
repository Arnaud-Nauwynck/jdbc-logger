package fr.an.tools.jdbclogger.calllogger.templaterecorder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * default filter for StackTraceElement
 */
public class StackElementFilter {

    public static final List<StackElementFilter> DEFAULT_IGNORED_STACKELEMENT_FILTER_LIST;
    public static final List<StackElementFilter> DEFAULT_STOPPER_STACKELEMENT_FILTER_LIST;
    public static final List<StackElementFilter> EMPTY_STOPPER_STACKELEMENT_FILTER_LIST = Collections.emptyList();
    static {
        List<StackElementFilter> tmpIgnoreList = new ArrayList<StackElementFilter>();
        tmpIgnoreList.add(createFromRegexps("sun\\.reflect\\..*", null, null));
        tmpIgnoreList.add(createFromRegexps("java\\.lang\\.reflect", "Method", "invoke.*"));
        tmpIgnoreList.add(createFromRegexps("fr\\.an\\.tools\\.jdbc\\..*", null, null));
        tmpIgnoreList.add(createFromRegexps("fr\\.an\\.shared\\.util\\..*", null, null));
        tmpIgnoreList.add(createFromRegexps("oracle\\..*", null, null));
        tmpIgnoreList.add(createFromRegexps("toplink\\..*", null, null));
        tmpIgnoreList.add(createFromRegexps("weblogic\\.jdbc\\..*", null, null));
        tmpIgnoreList.add(createFromRegexps("org\\.apache\\.commons\\.dbcp.*", null, null));

        tmpIgnoreList.add(createFromRegexps("org\\.springframework\\.aop\\.support", "AopUtils", null));
        tmpIgnoreList.add(createFromRegexps("org\\.springframework\\.aop\\.framework", null, null));
//        tmpIgnoreList.add(createFromRegexps("org\\.springframework\\.aop\\.framework", "ReflectiveMethodInvocation", null));
//        tmpIgnoreList.add(createFromRegexps("org\\.springframework\\.aop\\.framework", "JdkDynamicAopProxy", null));
        tmpIgnoreList.add(createFromRegexps("org\\.springframework\\.transaction.*", null, null));
        tmpIgnoreList.add(createFromRegexps("org\\.springframework\\.transaction\\.interceptor", "TransactionInterceptor", null)); 
        tmpIgnoreList.add(createFromRegexps("org\\.springframework\\.orm\\..*", null, null));
        
        DEFAULT_IGNORED_STACKELEMENT_FILTER_LIST = Collections.unmodifiableList(tmpIgnoreList);
        
        List<StackElementFilter> tmpStopList = new ArrayList<StackElementFilter>();

        DEFAULT_STOPPER_STACKELEMENT_FILTER_LIST = Collections.unmodifiableList(tmpStopList);
    }

    private Pattern packageNamePattern;
    private Pattern baseClassNamePattern;
    private Pattern methodNamePattern;

    // -------------------------------------------------------------------------

    /** full ctor */
    public StackElementFilter(Pattern packageNamePattern, Pattern baseClassNamePattern, Pattern methodNamePattern) {
        this.packageNamePattern = packageNamePattern;
        this.baseClassNamePattern = baseClassNamePattern;
        this.methodNamePattern = methodNamePattern;
    }

    /** ctor helper */
    public static StackElementFilter createFromRegexps(String packageNameRegexp,
                                                       String baseClassNameRegexp,
                                                       String methodNameRegexp) {
        Pattern packageNamePattern = (packageNameRegexp != null && !packageNameRegexp.equals("*")) ? Pattern.compile(packageNameRegexp)
                                                                                                  : null;
        Pattern baseClassNamePattern = (baseClassNameRegexp != null && !baseClassNameRegexp.equals("*")) ? Pattern.compile(baseClassNameRegexp)
                                                                                                        : null;
        Pattern methodNamePattern = (methodNameRegexp != null && !methodNameRegexp.equals("*")) ? Pattern.compile(methodNameRegexp)
                                                                                               : null;
        return new StackElementFilter(packageNamePattern, baseClassNamePattern, methodNamePattern);
    }

    /** list ctor helper */
    public static List<StackElementFilter> createListFromRegexpsTokenizers(String listText,
                                                                               String listElementDelimiter,
                                                                               String innerPatternDelimiter) {
        List<StackElementFilter> res = new ArrayList<StackElementFilter>();
        StringTokenizer listElementTokenizer = new StringTokenizer(listText, listElementDelimiter);
        for (; listElementTokenizer.hasMoreTokens();) {
            String listElementToken = listElementTokenizer.nextToken();

            String[] regexpArray = new String[3];
            StringTokenizer regexpTokenizer = new StringTokenizer(listElementToken, innerPatternDelimiter);
            for (int index = 0; regexpTokenizer.hasMoreTokens() && index < 3; index++) {
                String regexpToken = regexpTokenizer.nextToken();
                if (regexpToken.length() == 0 || regexpToken.equals("*") || regexpToken.equals(".*")) {
                    regexpToken = null;
                }
                regexpArray[index] = regexpToken;
            }

            StackElementFilter resElt = createFromRegexps(regexpArray[0], regexpArray[1], regexpArray[2]);
            res.add(resElt);
        }
        return res;
    }

    // -------------------------------------------------------------------------

    public boolean accept(StackTraceElement elt) {
        String fullClassName = elt.getClassName();
        int indexDot = fullClassName.lastIndexOf('.');

        if (packageNamePattern != null) {
            if (indexDot != -1) {
                String packageName = fullClassName.substring(0, indexDot);
                boolean res = packageNamePattern.matcher(packageName).matches();
                if (!res) {
                    return false;
                }
            }
        }

        if (baseClassNamePattern != null) {
            if (indexDot != -1) {
                String baseClassName = fullClassName.substring(indexDot + 1, fullClassName.length());
                boolean res = baseClassNamePattern.matcher(baseClassName).matches();
                if (!res) {
                    return false;
                }
            }
        }

        if (methodNamePattern != null) {
            if (indexDot != -1) {
                String methodName = elt.getMethodName();
                boolean res = methodNamePattern.matcher(methodName).matches();
                if (!res) {
                    return false;
                }
            }
        }

        return true;
    }

    /** helper to test at least one match in a list of filter */
    public static boolean acceptOneOf(List<StackElementFilter> filterList, StackTraceElement elt) {
        boolean res = false;
        if (filterList != null && !filterList.isEmpty()) {
            for (Iterator<StackElementFilter> iter = filterList.iterator(); iter.hasNext();) {
                StackElementFilter filter = iter.next();
                if (filter.accept(elt)) {
                    res = true;
                    break;
                }
            }
        }
        return res;
    }

}
