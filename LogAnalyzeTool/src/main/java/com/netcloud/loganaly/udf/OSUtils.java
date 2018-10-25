package com.netcloud.loganaly.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class OSUtils extends UDF {
    public String evaluate(String s)
    {
        if(s.toLowerCase().contains("windows"))
            return "Windows";
        else if(s.toLowerCase().contains("macos"))
            return  "MacOS";
        else if(s.toLowerCase().contains("linux"))
            return "Linux";
        else if(s.toLowerCase().contains("android"))
            return "Android";
        else if(s.toLowerCase().contains("ios"))
            return "IOS";
        else
            return "Others";
    }
}
