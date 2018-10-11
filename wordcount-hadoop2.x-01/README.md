…or create a new repository on the command line
echo "# sparkdemo" >> README.md
git init
git add README.md
git commit -m "first commit"
git remote add origin https://github.com/ysjyang/sparkdemo.git
git push -u origin master
…or push an existing repository from the command line
git remote add origin https://github.com/ysjyang/sparkdemo.git
git push -u origin master

1、每个mapper进程处理HDFS上一个Block的数据。
2、当Mapper进程启动后，其将会被频繁的调用用来处理Block文件中每行文本。每次调用时，传递给Mapper的键是文档这行数据的起始位置的字符偏移量，对应的值是这行对应的文本。
3、hadoop的神奇的地方在于后面的（sort）排序和（shuffle）重新分发过程。hadoop会按照键来对键值对进行排序，然后重新洗牌，将所有的具有相同的键的键值对分发到同一个Reducer中。
   这里有多种方式可以决定哪个Reducer获取哪个范围的键对应的数据。
4、使用hive操作同样能够达到单词统计的功能。而且不需要进行编译生成Jar文件。

CREATE TABLE docs (line STRING);
LOAD DATA INPATH '/usr/local/wordcount' OVERWRITE INTO TABLE docs;

CREATE TABLE word_count AS 
SELECT word,count(1) AS count FROM (SELECT explode(split(line,‘\t’)) AS word from docs) w 
GROUP BY word ORDER BY word;
