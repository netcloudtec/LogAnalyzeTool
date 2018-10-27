package com.netcloud.loganaly.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * @author Andy
 * UDF函数 实现的功能相当于 nvl()
 * @date 2018/10/26 22:39
 */
@Description(name = "nvl",
        value = "_FUNC_(value,default_value) - Return value is null return default_value else return value ",
        extended = "Example:\n" +
                "> select _FUNC_(null,'bla') from src limit 1;\n ")
public class GenericUDFNvl extends GenericUDF {

    private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    private ObjectInspector[] argementOIs;

    /*会被每个输入的参数最初调用，并且最终传入到一个ObjectInspector对象中，这个方法的目标就是确定参数的返回类型
     * 如果传入到方法中的参数是不合法的，就会抛出Exception异常信息
     * returnOIResolver是一个内置的类，其通过获取非null值的变量类型并使用这个变量的类型来确定返回值的类型。*/
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        argementOIs = arguments;
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The operator 'NVL' accepts 2 arguments.");
        }
        /*创建returnOIResolver对象
         * 1、判断传入参数的数据类型是否相同  returnOIResolver.update(arg[])
         * 2、返回输入参数的数据类型          returnOIResolver.get()*/
        returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
        if (!(returnOIResolver.update(arguments[0]) && returnOIResolver.update(arguments[1]))) {
            throw new UDFArgumentTypeException(2, "The 1st and 2nd args of function NLV should have same type" +
                    "but they are different:\"" + arguments[0].getTypeName() + "\" and \"" + arguments[1].getTypeName() + "\"");
        }
        return returnOIResolver.get();//返回输入参数的类型。
    }

    /*此函数返回非null值 DeferredObject[]对象中存储的是传入参数的值*/
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object retValue = returnOIResolver.convertIfNecessary(arguments[0].get(), argementOIs[0]);
        if (retValue == null) {
            retValue = returnOIResolver.convertIfNecessary(arguments[1].get(), argementOIs[1]);
        }
        return retValue;
    }
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("if");
        sb.append("children[0]");
        sb.append(" is null");
        sb.append("returns");
        sb.append("children[1]");
        return sb.toString();
    }
}
