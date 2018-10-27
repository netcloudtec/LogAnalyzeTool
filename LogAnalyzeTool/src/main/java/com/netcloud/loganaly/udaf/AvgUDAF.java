package com.netcloud.loganaly.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 计算平均数
 * 1.什么是UDAF
 * UDF只能实现一进一出的操作，如果需要实现多进一出，则需要实现UDAF。
 * 比如： Hive查询数据时，有些聚类函数在HQL没有自带，需要用户自定义实现； 用户自定义聚合函数:Sum, Average
 * 2.实现UFAF的步骤
 * 引入如下两下类
 * import org.apache.hadoop.hive.ql.exec.UDAF
 * import org.apache.hadoop.hive.ql.exec.UDAFEvaluator
 * 函数类需要继承UDAF类，计算类Evaluator实现UDAFEvaluator接口
 * Evaluator需要实现UDAFEvaluator的init、iterate、terminatePartial、merge、terminate这几个函数。
 * a）init函数实现接口UDAFEvaluator的init函数。
 * b）iterate接收传入的参数，并进行内部的迭代。其返回类型为boolean。
 * c）terminatePartial无参数，其为iterate函数遍历结束后，返回遍历得到的数据，terminatePartial类似于 hadoop的Combiner。
 * d）merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean。
 * e）terminate返回最终的聚集函数结果。
 * 3.Hive中使用UDAF
 * 将java文件编译成udaf_avg.jar
 * 进入hive客户端添加jar包
 * hive>add jar /home/hadoop/udaf_avg.jar
 * 创建临时函数
 * hive>create temporary function udaf_avg 'com.netcloud.loganaly.udaf.AvgUDAF'
 * 查询语句
 * hive>select udaf_avg(people.age) from people
 * 销毁临时函数
 * hive>drop temporary function udaf_avg
 * 4.总结
 * UDF是对只有单条记录的列进行的计算操作，而UDFA则是用户自定义的聚类函数，是基于表的所有记录进行的计算操作。
 * @author Andy
 * @date 2018/10/27 21:11
 */
public class AvgUDAF extends UDAF {
    public static class AvgState {
        private long mCount;
        private double mSum;
    }
    public static class AvgEvaluator implements UDAFEvaluator {
        AvgState state;
        public AvgEvaluator() {
            super();
            state = new AvgState();
            init();
        }
        /**
         * init函数类似于构造函数，用于UDAF的初始化
         */
        public void init() {
            state.mSum = 0;
            state.mCount = 0;
        }
        /**
         * iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean * * @param o * @return
         */

        public boolean iterate(Double o) {
            if (o != null) {
                state.mSum += o;
                state.mCount++;
            }
            return true;
        }
        /**
         * terminatePartial无参数，其为iterate函数遍历结束后，返回轮转数据， * terminatePartial类似于hadoop的Combiner * * @return
         */
        public AvgState terminatePartial() {
            // combiner
            return state.mCount == 0 ? null : state;
        }
        /**
         * merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean * * @param o * @return
         */
        public boolean merge(AvgState avgState) {
            if (avgState != null) {
                state.mCount += avgState.mCount;
                state.mSum += avgState.mSum;
            }
            return true;
        }
        /**
         * terminate返回最终的聚集函数结果 * * @return
         */
        public Double terminate() {
            return state.mCount == 0 ? null : Double.valueOf(state.mSum / state.mCount);
        }
    }
}

