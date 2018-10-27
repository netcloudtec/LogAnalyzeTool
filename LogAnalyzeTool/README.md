# logAnalyzeHelper
项目文档介绍[文档连接](https://blog.csdn.net/Oeljeklaus/article/details/80571519),
## 工程的目的
该工程是论坛日志分析系统的辅助工程，主要功能模块是帮助日志分析系统清洗数据、以及在Hive中建立相应的数据表。
## 工程目录结构
### /src
### ------/main
### --------------/java
### ---------------------/com.netcloud.loganaly
### ---------------------------------------/preprocess
### -------------------------------------------------/ClickStream_Mapper r日志清洗数据
### -------------------------------------------------/ClickStream_Reduce r日志清洗数据
### -------------------------------------------------/ClickStream_Driver r日志清洗数据
### ---------------------------------------/domain
### ------------------------------------------------/WebLogBean 日志的POJO对象
### ---------------------------------------/udf
### -------------------------------------------bean
### ---------------------------------------------Pair IP查找的辅助类
### -------------------------------------------BrowserUtils 获取浏览器的UDF函数
### -------------------------------------------CityUtils 获取城市的UDF函数类
### -------------------------------------------IPUtils  获取省份的UDF函数类
### -------------------------------------------OSUtils  获取操作系统UDF函数类
### -------------/resources
### ------/test
### ------/pom,xml 
将项目通过mavevn 打成jar包
hadoop jar com.netcloud.loganaly.preprocess.ClickStream_Driver  /hadoop/loganaly/testlog.txt  /output/loganaly/log123                  
## 工程使用的数据集
工程使用的数据集可以在作者的百度云中
下载[日志数据](https://pan.baidu.com/s/1ALZfXFkGcERiaQEIs6JHxQ),
## 工程的流程
原始数据--->数据规整--->ETL--->导入mysql数据库--->可视化
## ETL使用的SQL
使用的SQL在本本工程中命名为点击流.sql

用户自定义函数也包含在内

UDF的实现过程
1、继承UDF函数
2、重写evaluate方法.

UDAF的实现过程
 * 1.什么是UDAF
 * UDF只能实现一进一出的操作，如果需要实现多进一出，则需要实现UDAF。
 * 比如： Hive查询数据时，有些聚类函数在HQL没有自带，需要用户自定义实现； 用户自定义聚合函数:Sum, Average
 * 2.实现UFAF的步骤
 * 引入如下两下类
 * importorg.apache.hadoop.hive.ql.exec.UDAF
 * importorg.apache.hadoop.hive.ql.exec.UDAFEvaluator
 *函数类需要继承UDAF类，计算类Evaluator实现UDAFEvaluator接口
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

UDTF的实现过程
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

	
