package com.netcloud.loganaly.udf;


import org.apache.hadoop.hive.ql.exec.UDF;

public class BrowserUtils extends UDF {
    public String evaluate(String s) {
        if (s.toLowerCase().contains("chrome"))
            return "Chrome";
        else if (s.toLowerCase().contains("firefox"))
            return "Firefox";
        else if (s.toLowerCase().contains("mozilla"))
            return "Mozilla";
        else if (s.toLowerCase().contains("ie"))
            return "IE";
        else if (s.toLowerCase().contains("opera"))
            return "Oprea";
        else if (s.toLowerCase().contains("safari"))
            return "Safari";
        else if (s.toLowerCase().contains("uc"))
            return "UC";
        else if (s.toLowerCase().contains("qq"))
            return "QQ";
        else
            return "Others";
    }


}
