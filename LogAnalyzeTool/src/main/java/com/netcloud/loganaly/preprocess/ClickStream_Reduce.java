package com.netcloud.loganaly.preprocess;

import com.netcloud.loganaly.domain.WebLogBean;
import com.netcloud.loganaly.utils.Metadata;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ClickStream_Reduce extends Reducer<Text, WebLogBean, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> mos;
    private static Text v = new Text();


    protected void setup(Context context) {
        initResources(context);
    }

    private void initResources(Context context) {
        Metadata rt = new Metadata("/config/configuration.properties");
        mos = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<WebLogBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<WebLogBean> beans = new ArrayList<WebLogBean>();
        try {
            for (WebLogBean bean : values) {
                WebLogBean webLogBean = new WebLogBean();
                try {
                    BeanUtils.copyProperties(webLogBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                beans.add(webLogBean);
            }
            //将bean按时间先后顺序排序
            Collections.sort(beans, new Comparator<WebLogBean>() {
                public int compare(WebLogBean o1, WebLogBean o2) {
                    try {
                        Date d1 = toDate(o1.getTimeStr());
                        Date d2 = toDate(o2.getTimeStr());
                        if (d1 == null || d2 == null) return 0;
                        return d1.compareTo(d2);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return 0;
                    }
                }
            });
            /**
             * 以下逻辑为：从有序bean中分辨出各次visit，并对一次visit中所访问的page按顺序标号step
             * */
            int step = 1;
            String session = UUID.randomUUID().toString();
//            String TimeMill;

            for (int i = 0; i < beans.size(); i++) {
                WebLogBean bean = beans.get(i);
                if (1 == beans.size()) {
                    // 设置默认停留市场为60s
                    v.set(session + "," + bean.getIp() + "," + bean.getTimeStr() + "," + bean.getRequest_url() + "," + step + "," + (60) + "," + bean.getFrom_url() + "," + bean.getPlatform() + "," + bean.getBytes() + "," + bean.getStatus());
                    context.write(NullWritable.get(), v);
//                    mos.write(NullWritable.get(), v, "/user/root/output/loganaly/" + "10" + "/" + "08" + "/" + TimeMill);//输出到指定的路径下文件的名字是/output/loganaly/09/26/TimeMill-r-00000
                    session = UUID.randomUUID().toString();
                    break;
                }
                // 如果不止1条数据，则将第一条跳过不输出，遍历第二条时再输出
                if (i == 0) {
                    continue;
                }
                // 求近两次时间差
                long timeDiff = timeDiff(toDate(bean.getTimeStr()), toDate(beans.get(i - 1).getTimeStr()));
                // 如果本次-上次时间差<30分钟，则输出前一次的页面访问信息
                if (timeDiff < 30 * 60 * 1000) {
                    v.set(session + "," + beans.get(i - 1).getIp() + "," + beans.get(i - 1).getTimeStr() + "," + beans.get(i - 1).getRequest_url() + "," + step + "," + (timeDiff / 1000) + "," + beans.get(i - 1).getFrom_url() + "," + beans.get(i - 1).getPlatform() + "," + beans.get(i - 1).getBytes() + "," + beans.get(i - 1).getStatus());
                    context.write(NullWritable.get(), v);
//                    mos.write(NullWritable.get(), v, "/user/root/output/loganaly/" + "10" + "/" + "08" + "/" + TimeMill);//输出到指定的路径下文件的名字是/output/loganaly/09/26/TimeMill-r-00000
                    step++;
                } else {
                    // 如果本次-上次时间差>30分钟，则输出前一次的页面访问信息且将step重置，以分隔为新的visit
                    v.set(session + "," + beans.get(i - 1).getIp() + "," + "," + beans.get(i - 1).getRequest_url() + "," + step + "," + (60) + "," + beans.get(i - 1).getFrom_url() + "," + beans.get(i - 1).getPlatform() + "," + beans.get(i - 1).getBytes() + "," + beans.get(i - 1).getStatus());
                    context.write(NullWritable.get(), v);
//                    mos.write(NullWritable.get(), v, "/user/root/output/loganaly/" + "10" + "/" + "08" + "/" + TimeMill);//输出到指定的路径下文件的名字是/output/loganaly/09/26/TimeMill-r-00000
                    step = 1;
                    session = UUID.randomUUID().toString();
                }
                // 如果此次遍历的是最后一条，则将本条直接输出
                if (i == beans.size() - 1) {
                    // 设置默认停留市场为60s
                    v.set(session+","+bean.getIp() + "," + bean.getTimeStr() + "," + bean.getRequest_url() + "," + step + "," + (60) + "," + bean.getFrom_url() + "," + bean.getPlatform() + "," + bean.getBytes() + "," + bean.getStatus());
                    context.write(NullWritable.get(), v);//文件输出到指定的路径 文件的名字前缀part-r 为了对结果再进行区分以及修改文件的名字可以使用多目录输出。
//                    mos.write(NullWritable.get(), v, "/user/root/output/loganaly/" + "10" + "/" + "08" + "/" + TimeMill);//输出到指定的路径下文件的名字是/output/loganaly/09/26/TimeMill-r-00000
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static Date toDate(String timeStr) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA);
        return df.parse(timeStr);
    }

    private static long timeDiff(String time1, String time2) throws ParseException {
        Date d1 = toDate(time1);
        Date d2 = toDate(time2);
        return d1.getTime() - d2.getTime();
    }

    private static long timeDiff(Date time1, Date time2) throws ParseException {
        return time1.getTime() - time2.getTime();
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("============================successful====================================");
        mos.close();
    }

}
