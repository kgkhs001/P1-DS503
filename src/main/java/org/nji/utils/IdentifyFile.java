package org.nji.utils;

import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class IdentifyFile {
    public static enum File {Unknown, CircleNet, Follows, ActivityLog}
    static Pattern p = Pattern.compile("^\\d*$");
    static public File identify(String line){
        StringTokenizer st = new StringTokenizer(line, ",");
        String[] sArray = {st.nextToken(), st.nextToken(), st.nextToken(), st.nextToken()};
        if (p.matcher(sArray[1]).find() && p.matcher(sArray[2]).find() && p.matcher(sArray[3]).find()) {
            return File.Follows;
        } else if (p.matcher(sArray[1]).find() && p.matcher(sArray[2]).find() && !p.matcher(sArray[3]).find()) {
            return File.ActivityLog;
        } else if (!p.matcher(sArray[1]).find() && !p.matcher(sArray[2]).find()) {
            return File.CircleNet;
        } else {
            return File.Unknown;
        }

    }
}
