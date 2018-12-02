package fr.an.tools.jdbclogger.calllogger.templaterecorder;

import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class TemplateSqlUtil {

    private static final String[] IN_PARENTHESIS = new String[] { "in (", "IN (", "in  (" };
    private static final String[] SELECT = new String[] { "select ", "SELECT ", "Select " };
    private static final String[] INSERT = new String[] { "insert ", "INSERT ", "Insert " };
    private static final String[] UPDATE = new String[] { "update ", "UPDATE ", "Update " };
    private static final String[] VALUES = new String[] { "values (", "VALUES (", "Values (" };
    private static final String[] SET = new String[] { "set ", "SET ", "Set " };
    private static final String[] WHERE = new String[] { "where", "WHERE" };
    private static final String[] FROM = new String[] { "from", "FROM" };
    private static final String[] FROM_SELECT = new String[] { "FROM (SELECT", "from (select" };
    
    private static final String ws = "[ \t\r\n]*";
    private static final String wsOrSep = "[, \t\r\n][ \t\r\n]*";
    
    private static final String TK_IN = "[Ii][Nn]";
    private static final String TK_NOT_IN = "[Nn][Oo][Tt]" + ws + "[Ii][Nn]";
    
    // String tokenRE = "[^ \\t\\r\\n,\\)]*";
    private static final String dateRE = "[0-9/-] + ";
    public static final String comparatorRE = "(=|>|=|<=|>=|\\!=|[Ii][Ss]|[Ll][Ii][Kk][Ee])";
    private static final String nameRe = "[A-Za-z_][A-Za-z_0-9]*";
    private static final String floatRe = "[+-]?[0-9]+\\.[0-9]*";

    private static final String openOrComma = "\\([\\\\(,]\\)";
    private static final String closeOrComma = "\\([\\\\),]\\)";
    private static final String parenthesedExprs = "\\([^)]*\\)";
    private static final String digit = "[0-9]";
    private static final String digits = "[0-9]+";
    private static final Pattern numberPattern = Pattern.compile(ws + digits + ws);
    private static final String decimal = digit + "[,.]" + digit + "*";
    private static final String numberList = ws + digits + ws + "(," + ws + digits + ")*";
    private static final String litteral = "'[^']*'";
    private static final Pattern litteralPattern = Pattern.compile(ws + litteral + ws);
    private static final String litteralList = ws + litteral + ws + "(," + ws + litteral + ")*";
    private static final String resSlot = "\\1 ? \\2";

    public static class PatternReplacement {
        String regex;
        Pattern pattern;
        String replacement;

        public PatternReplacement(String regex, String replacement) {
            this.regex = regex;
            this.pattern = Pattern.compile(regex);
            this.replacement = replacement;
        }
    }

    private static final PatternReplacement[] sqlPatternReplacements = new PatternReplacement[] {
        new PatternReplacement("'[0-9: \t]*'", "'?'"),

        new PatternReplacement("\\)" + ws + "values" + ws + "\\(.*\\)", ") values (..)"),

        new PatternReplacement("([\\(=<>,])" + ws + dateRE + ws, "$1 ? "),

        new PatternReplacement("([\\(=<>,])" + ws + "-?[0-9]+[,\\.][0-9]*" + ws, "$1 ? "),
        new PatternReplacement("([\\(=<>,])" + ws + "-?[0-9]+" + ws, "$1 ? "),
        new PatternReplacement("([\\(=<>,])" + ws + litteral + ws, "$1 ? "),

        new PatternReplacement(ws + "[Tt]o_[Tt]imestamp" + ws + "\\(" + ws + litteral + ws + "," + ws + litteral + ws + "\\)", 
        			" to_timestamp(?,'') "),
        new PatternReplacement(ws + "[Tt][Oo]_[Dd][Aa][Tt][Ee]" + ws + "\\(" + ws + litteral + ws + "," + ws + litteral + ws + "\\)", 
        			" to_date(?,'') "),

        new PatternReplacement("decode" + ws + parenthesedExprs, "decode(?...) "),

        new PatternReplacement("=" + ws + "\\([^\\)]*\\)", "= ? "),

        new PatternReplacement("set " + nameRe + ws + "=" + ws + nameRe + "[+-]" + floatRe, "set ..=..+?"),

        new PatternReplacement("round" + ws + "\\([^\\)]*\\)", "round(..)"),
        new PatternReplacement("datediff" + ws + "\\([^\\)]*\\)", "datediff(..)"),

        // TODO replace in "values ( ... )" 
        new PatternReplacement(openOrComma + ws + digits + ws + closeOrComma, resSlot),
        new PatternReplacement(openOrComma + ws + decimal + ws + closeOrComma, resSlot),
        new PatternReplacement(openOrComma + ws + litteral + ws + closeOrComma, resSlot),

        new PatternReplacement("in \\(" + ws + litteralList + ws + "\\)", "in { '?', '?'... }"),
        new PatternReplacement("in \\(" + ws + numberList + ws + "\\)", "in { ?, ?... }"), // ==> Crash with list too longs: StackOverflowError  
        new PatternReplacement(wsOrSep + digits + ws + TK_NOT_IN + ws + "\\(", "? not in("),  
        new PatternReplacement(wsOrSep + digits + ws + TK_IN + ws + "\\(", "? in("),  

        new PatternReplacement(ws + digits + ws + ",", " ?,")  
    };

    public static String templatizeSqlText(String sql) {
        String text = sql; // ALREADY called.. removeNewLineAndLineComments(sql);
        
        if (text.startsWith(" ")) {
        	text = text.trim(); 
        }

        text = templatizeSqlText(text, 0);

        return text;
    }

    public static String templatizeSqlText(String sql, int startIndex) {
        String text = sql;

        int nextSelectIndex = indexOfAny(text, SELECT, startIndex);
        if (nextSelectIndex >= startIndex) {
            int indexFrom = indexOfAny(text, FROM, 0);
            //TODO truncate long column lists...
            String selectPart;
            if (indexFrom != -1) {
                if (indexFrom < 50) {
                    selectPart = text.substring(0, indexFrom);
                } else {
                    selectPart = text.substring(0, 30) + " ... "
                    //                        + text.substring(indexFrom-10, indexFrom)
                    ;
                }
                
                int indexFromSelect = indexOfAny(text, FROM_SELECT, indexFrom);
                if (indexFromSelect != -1) {
                	// => recurse templatize select!
                	String rightPart = text.substring(indexFromSelect + 6, text.length());
                	String rightTemplate = templatizeSqlText(rightPart, 0); // *** recurse ***
                	return selectPart + " " + rightTemplate;
                }

                int indexWhere = indexOfAny(text, WHERE, indexFrom);
                if (indexWhere != -1) {
                    String fromPart = text.substring(indexFrom, indexWhere);

                    // now replace all number by ?, and all '[^']*' by '?'
                    String wherePart = text.substring(indexWhere, text.length());
                    StringBuilder wherePartBuffer = new StringBuilder(text.length());
                    wherePartBuffer.append(selectPart);
                    wherePartBuffer.append(fromPart);

                    //                    wherePart = removeInList(wherePart);

                    //                    int indexInParenth = indexOfAny(wherePart, IN_PARENTHESIS, 0);
                    //                    while(indexInParenth != -1) {
                    //                        int indexCloseParenth = wherePart.indexOf(")", indexInParenth);
                    //                        if (indexCloseParenth == -1) {
                    //                            break;
                    //                        }
                    //                        wherePart = wherePart.substring(0, indexInParenth) + " in (?..)" + wherePart.substring(indexCloseParenth+1, wherePart.length()); 
                    //                        
                    //                        indexInParenth = indexOfAny(wherePart, IN_PARENTHESIS, indexInParenth + 9);
                    //                    }

                    final int size = wherePart.length();
                    for (int i = 0; i < size; i++) {
                        char ch = wherePart.charAt(i);
                        if (ch == '\'') {
                            // find next matching "'".. replace content
                            int newI = wherePart.indexOf('\'', i + 1);
                            if (newI == -1) {
                                System.err.println("missing closing ' for sql text : " + sql);
                                break;
                            } else {
                                i = newI;
                            }
                            wherePartBuffer.append("'?'");
                        } else if (ch == '>' || ch == '<' || ch == '=') {
                            wherePartBuffer.append(ch);
                            if (i + 1 == size) {
                                System.err.println("error.. sql text ending by <>=");
                            }
                            ch = wherePart.charAt(++i);
                            if (i + 1 < size && ch == '=') {
                                wherePartBuffer.append(ch);
                                ch = wherePart.charAt(++i);
                            }
                            // skip whitespace
                            if (i + 1 < size && Character.isWhitespace(ch)) {
                                wherePartBuffer.append(ch);
                                ch = wherePart.charAt(++i);
                            }
                            // skip numerics values
                            if (((ch == '+' || ch == '-') && i + 1 < size && Character.isDigit(wherePart.charAt(i + 1)))
                                || Character.isDigit(ch)) {
                                wherePartBuffer.append('?');
                                if (ch == '+' || ch == '-') {
                                    ch = wherePart.charAt(++i);
                                }
                                // skip remaining digits
                                for (; i < size && Character.isDigit(wherePart.charAt(i)); i++) {
                                }
                                if (i < size) {
                                    ch = wherePart.charAt(i);
                                    wherePartBuffer.append(ch);
                                }

                            } else if (Character.isDigit(ch)) {
                                wherePartBuffer.append('?');
                                ch = wherePart.charAt(++i);
                                // skip remaining digits
                                for (; i < size && Character.isDigit(ch); i++, ch = wherePart.charAt(i)) {
                                }
                                wherePartBuffer.append(ch);
                            } else if (ch == '\'') {
                                // find next matching "'".. replace content
                                i = wherePart.indexOf('\'', i + 1);
                                wherePartBuffer.append("'?'");
                            } else {
                                wherePartBuffer.append(ch);
                            }
                        } else {
                            wherePartBuffer.append(ch);
                        }
                    }
                    text = wherePartBuffer.toString();

                    // return text; // return to avoid Regexp CONSUMING CPU

                } else {
                    // !! did not find where clause!!
                }

            } else {
                // not a table select?
            }
        } else if (indexOfAny(text, INSERT, startIndex) == startIndex) {
            int firstLeftBracket = text.indexOf("(");
            if (firstLeftBracket != -1) {
                int valuesIndex = indexOfAny(text, VALUES, 0);
                if (valuesIndex != -1) {
                    // truncate optional column names on left of "values (" 
                    int firstRightBracket = text.lastIndexOf(")", valuesIndex);
                    if (firstRightBracket != -1) {
                        if (firstRightBracket - firstLeftBracket > 30) {
                            text = text.substring(0, firstLeftBracket + 30) + "..."
                                   + text.substring(firstRightBracket, text.length());
                            firstRightBracket = text.indexOf(")", firstLeftBracket + 1);
                            valuesIndex = indexOfAny(text, VALUES, 0);
                        }
                    }

                    text = text.substring(0, valuesIndex) + "values (?...)";
                    return text;
                }
            }
        } else if (indexOfAny(text, UPDATE, startIndex) == startIndex) {
            int setIndex = indexOfAny(text, SET, 0);
            if (setIndex != -1) {
                String updatePart = text.substring(startIndex, setIndex+4);
                String setPart = text.substring(setIndex, text.length());
                String setPartReplaced = getTextWithSqlPatternsReplaced(setPart);
                return updatePart + setPartReplaced; 
            }
        }
        
        try {
        	text = removeInList(text);
        } catch(Exception ex) {
        	// ignore!!!
        }
        
        text = getTextWithSqlPatternsReplaced(text);

        return text;
    }

    protected static String getTextWithSqlPatternsReplaced(String text) {
        final int sqlPatternReplacementsSize = sqlPatternReplacements.length;
        for (int i = 0; i < sqlPatternReplacementsSize; i++) {
            PatternReplacement rep = sqlPatternReplacements[i];
            try {
                text = rep.pattern.matcher(text).replaceAll(rep.replacement);
            } catch (Throwable ex) {
                System.err.println("Failed to replace text=" + text + "\n" + "regex=" + rep.regex + "\n" + "regexp="
                                   + rep.pattern.toString() + "\n" + "by=" + rep.replacement);
                ex.printStackTrace(System.err);
            }
        }
        return text;
    }

    private static String removeInList(String text) {
        int startIndex = indexOfAny(text, IN_PARENTHESIS, 0);
        if (startIndex == -1) {
            return text;
        }

        for (; startIndex != -1; startIndex = indexOfAny(text, IN_PARENTHESIS, startIndex + 4)) {
            int endIndex = text.indexOf(")", startIndex + 1);
            startIndex = text.indexOf("(", startIndex + 1); // handle whitespaces between "in" and "("

            String subStr = text.substring(startIndex + 1, endIndex);
            if (startsWithAny(subStr, SELECT)) {
                // detected "in (select ..)"
                continue;
            }

            // TODO also test for pair lists...
            // ((v1,v2), (v3,v4), ...) !!!
            if (text.charAt(startIndex + 1) == '(') {
                endIndex = text.indexOf("))");
                if (endIndex != -1) {
                    text = text.substring(0, startIndex) + " in ((?,?..), (?,?..), ..)" + text.substring(endIndex + 1);
                }
                continue;
            }

            boolean foundOnlyNumeric = true;
            StringTokenizer tokenizer = new StringTokenizer(subStr, ",");
            for (; tokenizer.hasMoreTokens();) {
                String token = tokenizer.nextToken();
                if (!numberPattern.matcher(token).matches()) {
                    foundOnlyNumeric = false;
                    break;
                }
            }
            if (foundOnlyNumeric) {
                text = text.substring(0, startIndex) + " in (?,? ... )" + text.substring(endIndex + 1);
            } else {
                // also test for literrals, delimited by ' '
                boolean foundOnlyLiterral = true;
                tokenizer = new StringTokenizer(subStr, ",");
                for (; tokenizer.hasMoreTokens();) {
                    String token = tokenizer.nextToken();
                    if (!litteralPattern.matcher(token).matches()) {
                        foundOnlyLiterral = false;
                        break;
                    }
                }
                if (foundOnlyLiterral) {
                    text = text.substring(0, startIndex) + " in ('?','?'... )" + text.substring(endIndex + 1);
                } else {
                    // unrecognized!!
                }
            }

        }

        return text;
    }

    /** String utility */
    public static int indexOfAny(String text, String[] searchText, int startIndex) {
        int res = -1;
        final int size = searchText.length;
        for (int i = 0; i < size; i++) {
            int tmp = text.indexOf(searchText[i], startIndex);
            if (tmp != -1) {
                if (res == -1) {
                    res = tmp;
                } else {
                    res = Math.min(res, tmp);
                }
            }
        }
        return res;
    }

    /** String utility */
    public static boolean startsWithAny(String text, String[] searchText) {
        boolean res = false;
        final int size = searchText.length;
        for (int i = 0; i < size; i++) {
            boolean tmp = text.startsWith(searchText[i]);
            if (tmp) {
                res = true;
                break;
            }
        }
        return res;
    }

    /**
     * 
     * @param sql
     * @param removeSqlLineComment
     * @param heuristicNoNewLineInSqlComment HARD-CODED rule for removing pseudo line-number comments
     *         problem: cariage return are lost in file... so the comment contains in fact all the remaining sql text
     *         heuristic: sometimes, the comment is only a line number: "--[0-9]*"  
     * @return
     */
    public static String removeNewLineAndLineComments(String sql,
                                                      boolean removeSqlLineComment,
                                                      boolean heuristicNoNewLineInSqlComment) {
        // before suppressing newlines... must suppress single line comments "--" !
        // problem: protected strings as " ' -- ' " would be recognized as a comment!
        //   => must also detect begin/end ', and begin/end " !!
        // also suppress nexlines and multiple spaces in same step
        StringBuilder textSb = new StringBuilder(sql.length());
        {
            final int sqlSize = sql.length();
            char[] chArray = sql.toCharArray();
            for (int i = 0; i < sqlSize; i++) {
                char ch = chArray[i];
                switch (ch) {
                    case '-':
                        if (i + 1 < sqlSize && chArray[i + 1] == '-') {
                            // found SQL comment "--" 
                            if (removeSqlLineComment) {
                                // copy all until next end of line / or remove it?? 
                                // iterate until eol
                                textSb.append(ch);
                                ch = chArray[++i];
                                textSb.append(ch);

                                for (; i < sqlSize && chArray[i] != '\n'; i++) {
                                }
                                textSb.append("/*comment ?*/");
                                continue;

                            } else if (heuristicNoNewLineInSqlComment) {
                                i += 2; // skip first and second '-'
                                if (i + 1 < sqlSize && Character.isDigit(chArray[i])) {
                                    // found heuristic : detected line number comment, add newline
                                    // textSb.append('-');
                                    // textSb.append('-');
                                    for (; i < sqlSize && Character.isDigit(chArray[i]); i++) {
                                        // textSb.append(chArray[i]);
                                    }
                                    // textSb.append('\n');
                                    continue; // continue parsing + printing.. add supposed new line
                                } else {
                                    // no heuristic.... print full comment, no print newline! 
                                    textSb.append(ch);
                                    ch = chArray[++i];
                                    textSb.append(ch);
                                    break; // continue parsing + printing!
                                }

                            } else {
                                // do nothing!
                                // SQL will probably will impossible to templatize using regular expressions
                                textSb.append(ch);
                                // continue parsing + printing!
                                break;
                            }
                        }
                        break;

                    case '\'':
                        // skip text until closing ' (but protect \') !
                        i++;
                        textSb.append(ch);
                        for (; i < sqlSize && !(chArray[i] == '\'' && chArray[i - 1] != '\\'); i++) {
                            if (chArray[i] != '\n') {
                                textSb.append(chArray[i]);
                            } else {
                                textSb.append("\\n");
                            }
                        }
                        textSb.append(ch);
                        break;

                    case ' ':
                    case '\t':
                        // replace multiple spaces by 1
                        if (i + 1 < sqlSize && (chArray[i + 1] == ' ' || chArray[i + 1] == '\t')) {
                            int nextCh = chArray[i + 1];
                            if (nextCh == ' ' || nextCh == '\t') {
                                for (; i + 1 < sqlSize && (chArray[i + 1] == ' ' || chArray[i + 1] == '\t'); i++) {
                                }
                            }
                        }
                        textSb.append(ch);
                        break;

                    case '\r':
                        break;

                    case '\n':
                        // remove newline
                        // textSb.append("/*nl*/");
                        // continue;
                        textSb.append(' ');
                        break;

                    default:
                        textSb.append(ch);
                        break;
                }

            } //for i < sqlSize
        }
        String text = textSb.toString();
        return text;
    }

}
