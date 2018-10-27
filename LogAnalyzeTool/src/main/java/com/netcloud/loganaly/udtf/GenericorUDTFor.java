package com.netcloud.loganaly.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;

/**
 * @author Andy
 * 1. 什么是UDTF
 * UDTF，是User Defined Table-Generating Functions，一眼看上去，貌似是用户自定义生成表函数，
 * 这个生成表不应该理解为生成了一个HQL Table， 貌似更应该理解为生成了类似关系表的二维行数据集
 * 2. 如何实现UDTF
 * 继承org.apache.hadoop.hive.ql.udf.generic.GenericUDTF。
 * 实现initialize, process, close三个方法
 * UDTF首先会调用initialize方法，此方法返回UDTF的返回行的信息（返回个数，类型）。
 * 初始化完成后，会调用process方法，对传入的参数进行处理，可以通过forword()方法把结果返回。
 * 最后close()方法调用，对需要清理的方法进行清理.
 * 3.如何使用UDTF
 *  3.1 在select中使用UDTF
 *  select forx(1,5) as col0 from my_table;
 *  不可以添加其他字段使用：select a, forx(1,5) as col0 from my_table;
 *  不可以嵌套调用：select forx(forx(1,5)) as col0 from my_table;
 *  不可以和group by/cluster by/distribute by/sort by一起使用：
 *  select a, forx(1,5) as col0 from my_table group by col0;
 *  3.2 结合lateral view使用
 *  select my_table.name, myview.col1 from my_table lateral view forx(1,5) myview as col1;
 * 4.总结
 * 使用lateral view之后，那么col1相当于普通的列，可以参与查询，计算
 * @date 2018/10/27 19:55
 */

public class GenericorUDTFor extends GenericUDTF {
    IntWritable start;
    IntWritable end;
    IntWritable inc;
    Object[] forwardObj = null;

    @Override
    public void close() throws HiveException {
        // TODO Auto-generated method stub
    }

    /**
     * 函数返回一行数据 这行数据的数据类型确定是整型的，我们需要提供一个列名 ，
     * 用户也可以重新命名列名。
     *
     * @param args
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length > 3) {
            throw new UDFArgumentLengthException("此方法最多接收三个参数");
        }
        /*判断输入的参数类型是否是原生的参数类型*/
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("此类需要输入参数是Int类型");
        }
        start = ((WritableConstantIntObjectInspector) args[0]).getWritableConstantValue();
        end = ((WritableConstantIntObjectInspector) args[1]).getWritableConstantValue();
        if (args.length == 3) {
            inc = ((WritableConstantIntObjectInspector) args[2]).getWritableConstantValue();
        } else {
            inc = new IntWritable(1);//默认设置步长为1
        }
        this.forwardObj = new Object[1];//实例化forwardObj对象
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("col0");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * process方法是数据处理的过程 这个方法的返回值为void
     * 这种情况下会在for循环中对forward方法进行多次调用，每次调用获取一行的数据
     *
     * @param args
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {
        for (int i = start.get(); i < end.get(); i = i + inc.get()) {
            this.forwardObj[0] = new Integer(i);
            forward(forwardObj);
        }
    }
}