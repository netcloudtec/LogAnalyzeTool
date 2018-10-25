package com.netcloud.loganaly.udf;

import com.netcloud.loganaly.udf.bean.Pair;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class IPUtils extends UDF {

    static ArrayList<HashMap<Pair<Long, Long>, String>> ips = new ArrayList<HashMap<Pair<Long, Long>, String>>();

    static {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream("/home/hadoop/ip.txt")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                String[] splits = line.split("\\|");
                Long up = Long.parseLong(splits[2]);
                Long down = Long.parseLong(splits[3]);
                Pair<Long, Long> pair = new Pair<Long, Long>();
                pair.setFirst(up);
                pair.setSecond(down);
                StringBuilder sb = new StringBuilder();
                sb.append(splits[6]).append("|" + splits[7]);
                HashMap<Pair<Long, Long>, String> ip = new HashMap<Pair<Long, Long>, String>();
                ip.put(pair, sb.toString());
                ips.add(ip);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //获取省份和城市
    public static synchronized String getProvinceAndCity(String ip) {
        String[] splits = ip.split("\\.");
        double value = 0;
        for (int i = 0; i < splits.length; i++) {
            value += Long.parseLong(splits[i]) * Math.pow(2, 8 * (3 - i));
        }
        int high = ips.size() - 1;
        int low = 0;
        while (low <= high) {
            int mid = (low + high) / 2;
            Pair<Long, Long> pair = (Pair<Long, Long>) ips.get(mid).keySet().toArray()[0];
            if (value >= pair.getFirst() && value <= pair.getSecond()) {
                return (String) ips.get(mid).values().toArray()[0];
            } else if (value > pair.getSecond()) {
                low = mid + 1;
            } else if (value < pair.getFirst()) {
                high = mid - 1;
            }
        }
        return "未知|未知";
    }

    public synchronized String evaluate(String s) {
        return getProvinceAndCity(s).split("\\|")[0];
    }


}
