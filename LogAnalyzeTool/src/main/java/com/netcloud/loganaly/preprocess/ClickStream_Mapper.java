package com.netcloud.loganaly.preprocess;

import com.netcloud.loganaly.domain.WebLogBean;
import com.netcloud.loganaly.utils.Metadata;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;


public class ClickStream_Mapper extends Mapper<LongWritable, Text, Text, WebLogBean> {

    private static String IP;
    private static String TIME;
    private static String METHOD;
    private static String REQUEST_URL;
    /**
     * 使用的协议
     */
    private static String PROTOCOL;
    private static int STATUS;
    /**
     * 字节数
     */
    private static int BYTES;
    /**
     * 来源URL
     */
    private static String FROM_URL;
    /**
     * 使用的平台
     */
    private static String PLAT_FROM;

    private static int MIN_LENGTH;
    private static Text outKey = new Text();

    /**
     * 任务一旦开始就会执行这个方法
     */
    protected void setup(Context context) {
        initResources(context);
    }

    private void initResources(Context context) {
        Metadata rt = new Metadata("/config/configuration.properties");
        MIN_LENGTH = Integer.parseInt(rt.getValue("MIN_LENGTH"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String message = value.toString();
            String[] splits = message.split(" ");
            if (splits.length < MIN_LENGTH) return;
            IP = splits[0];
            TIME = formatDate(splits[3].substring(1));
            METHOD = splits[5].substring(1);
            REQUEST_URL = splits[6];
            PROTOCOL = StringUtils.isBlank(splits[7]) ? "HTTP/1.1" : splits[7].substring(0, splits[7].length() - 1);
            STATUS = StringUtils.isBlank(splits[8]) ? 0 : Integer.parseInt(splits[8]);
            BYTES = StringUtils.isBlank(splits[9]) ? 0 : Integer.parseInt(splits[9]);
            FROM_URL = StringUtils.isBlank(splits[9]) ? "-" : splits[10].substring(1, splits[10].length() - 1);
            StringBuilder sb = new StringBuilder();
            for (int i = 11; i < splits.length; i++) {
                sb.append(splits[i]);
            }
            String s = sb.toString();
            PLAT_FROM = s.substring(1, s.length() - 1);
            WebLogBean ms = new WebLogBean(IP, TIME, METHOD, REQUEST_URL, PROTOCOL, STATUS, BYTES, FROM_URL, PLAT_FROM);
            outKey.set(IP);
            context.write(outKey, ms);
        } catch (Exception e) {
            return;
        }
    }

    /**
     * params dateStr  04/Jan/2012:00:00:02
     * */
    public static String formatDate(String dateStr) {
        if (dateStr == null || StringUtils.isBlank(dateStr)) return "2018-09-28 00:00:02";
        SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA);
        String result = null;
        try {
            Date date = format.parse(dateStr);//将字符串转为指定格式的日期
            result = format1.format(date);//日期转为特定格式的字符串
        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            return result;
        }
    }


}
