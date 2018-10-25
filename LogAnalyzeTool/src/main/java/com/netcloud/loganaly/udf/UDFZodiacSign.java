package com.netcloud.loganaly.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Andy
 * @date 2018/10/25 23:00
 */
@Description(name = "zodiac",
        value = "_FUNC_(date) - from the input date string or separate month and day arguments,  return  the sign of the Zodiac.",
        extended = "Example:\n" +
                "> select _FUNC_(date_string) from src;\n " +
                "> select _FUNC_(month,day) from src;")
public class UDFZodiacSign extends UDF {

    private SimpleDateFormat df;

    public UDFZodiacSign() {
        df = new SimpleDateFormat("yyyy-MM-dd");
    }

    /*传入的参数是日期*/
    public String evaluate(Date date) {
        return this.evaluate(date.getMonth()+1, date.getDate());
    }

    /*传入的参数是日期格式的字符串*/
    public String evaluate(String bdate) {
        Date date = null;
        try {
            date = df.parse(bdate);

        } catch (Exception e) {
            return null;
        }
        return this.evaluate(date.getMonth() + 1, date.getDate());
    }

    public String evaluate(Integer month, Integer day) {
        String xz = null;
        if ((month == 3 && day >= 21 && day <= 31) || (month == 4 && day >= 1 && day <= 19)) {
            xz = "白羊座";
        }
        if ((month == 4 && day >= 20 && day <= 30) || (month == 5 && day >= 1 && day <= 20)) {
            xz = "金牛座";
        }
        if ((month == 5 && day >= 21 && day <= 31) || (month == 6 && day >= 1 && day <= 21)) {
            xz = "双子座";
        }
        if ((month == 6 && day >= 22 && day <= 30) || (month == 7 && day >= 1 && day <= 22)) {
            xz = "巨蟹座";
        }
        if ((month == 7 && day >= 23 && day <= 31) || (month == 8 && day >= 1 && day <= 22)) {
            xz = "狮子座";
        }
        if ((month == 8 && day >= 23 && day <= 31) || (month == 9 && day >= 1 && day <= 22)) {
            xz = "处女座";
        }
        if ((month == 9 && day >= 23 && day <= 30) || (month == 10 && day >= 1 && day <= 23)) {
            xz = "天秤座";
        }
        if ((month == 10 && day >= 24 && day <= 31) || (month == 11 && day >= 1 && day <= 22)) {
            xz = "天蝎座";
        }
        if ((month == 11 && day >= 23 && day <= 30) || (month == 12 && day >= 1 && day <= 21)) {
            xz = "射手座";
        }
        if ((month == 12 && day >= 22 && day <= 31) || (month == 1 && day >= 1 && day <= 19)) {
            xz = "摩羯座";
        }
        if ((month == 1 && day >= 20 && day <= 31) || (month == 2 && day >= 1 && day <= 18)) {
            xz = "水瓶座";
        }
        if ((month == 2 && day >= 19 && day <= 29) || (month == 3 && day >= 1 && day <= 20)) {
            xz = "双鱼座";
        }
        return xz;
      }
    public static void main(String[] args) throws ParseException {
        UDFZodiacSign udf=new UDFZodiacSign();
        System.out.println(udf.evaluate("2018-08-09"));
        String str = "2012-01-13";  //要跟上面sdf定义的格式一样
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

        Date today = df.parse(str);
        System.out.println(udf.evaluate(today));
    }
}
