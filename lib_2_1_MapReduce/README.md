# 大数据运算系统MapReduce实验记录

###### 梁宇龙	2023年4月10日于中国科学院大学

## 一、容器环境初始化

启动SSH服务：

```shell
root@<container ID>:~/hw2/part1# service ssh start
 * Starting OpenBSD Secure Shell server sshd                             [ OK ] 
```

启动HDFS：

```shell
root@<container ID>:~/hw2/part1# start-dfs.sh
Starting namenodes on [localhost]
localhost: starting namenode, logging to /home/bdms/setup/hadoop-2.9.2/logs/hadoop-root-namenode-36f965b5aab0.out
localhost: starting datanode, logging to /home/bdms/setup/hadoop-2.9.2/logs/hadoop-root-datanode-36f965b5aab0.out
Starting secondary namenodes [0.0.0.0]
0.0.0.0: starting secondarynamenode, logging to /home/bdms/setup/hadoop-2.9.2/logs/hadoop-root-secondarynamenode-36f965b5aab0.out
```

启动Yarn：

```shell
root@<container ID>:~/hw2/part1# start-yarn.sh
starting yarn daemons
starting resourcemanager, logging to /home/bdms/setup/hadoop-2.9.2/logs/yarn--resourcemanager-36f965b5aab0.out
localhost: starting nodemanager, logging to /home/bdms/setup/hadoop-2.9.2/logs/yarn-root-nodemanager-36f965b5aab0.out
```

## 二、对示例程序WordCount的解析

数据准备：

```shell
root@<container ID>:~/hw2/part1# hdfs dfs -rm -f -r /hw2/output
root@<container ID>:~/hw2/part1# hadoop fs -mkdir /hw2
root@<container ID>:~/hw2/part1# hadoop fs -put example-input.txt /hw2
root@<container ID>:~/hw2/part1# hadoop fs -ls -R /hw2
-rw-r--r--   1 root supergroup        165 2023-04-09 12:56 /hw2/example-input.txt
```

试运行代码：

```shell
root@<container ID>:~/hw2/part1# rm -f *.class *.jar
root@<container ID>:~/hw2/part1# javac WordCount.java
root@<container ID>:~/hw2/part1# jar cfm WordCount.jar WordCount-manifest.txt WordCount*.class
root@<container ID>:~/hw2/part1# ls
README.txt                       WordCount-manifest.txt  example-input.txt
WordCount$IntSumCombiner.class   WordCount.class         part1-input
WordCount$IntSumReducer.class    WordCount.jar
WordCount$TokenizerMapper.class  WordCount.java
root@<container ID>:~/hw2/part1# hadoop jar ./WordCount.jar /hw2/example-input.txt /hw2/output
root@<container ID>:~/hw2/part1# hdfs dfs -cat '/hw2/output/part-*'
count of 1.2.3.4 =	4
count of 10.3 =	1
count of 18811001100 =	2
count of 3 =	1
count of 30.1 =	1
count of 5.0 =	1
count of 5.2 =	1
count of 52 =	1
count of alpha =	4
count of beta =	2
```

核心代码分析：

1、文档输入后如何存储？在"TokenizerMapper"中的map方法参数中第一次出现文档表现形式："value"。

```java
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {...}
```

2、文档如何实现分词？在"StringTokenizer"对象创建过程中，通过指定"delim"参数的值确定分隔符实现分词。

```java
StringTokenizer itr = new StringTokenizer(value.toString()," ");
```

3、分词后如何依次写入Map？通过nextToken()方法逐个写入。

```java
while (itr.hasMoreTokens()) {
  word.set(itr.nextToken());
  context.write(word, one);
}
```

## 三、编写工程代码

1、Map函数

```java
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  //获取通话记录
  StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
  //对每条通话记录进行处理
  while (itr.hasMoreTokens()) {
    //筛选通话双方与通话时长
    String[] record = itr.nextToken().split(" ");
    //筛查脏数据:长度为3表示通话双方+通话时长
    if(record.length == 3){
      try {
        //通话时长是否可解析,捕捉不可解析错误跳过该记录
        Float.parseFloat(record[2]);
        token.set(record[0]+" "+record[1]);
        //用来存储结果,理想状态应使用FloatWritable存储,但在Reducer中无法合并,故使用Test存储本阶段Time数据
        time.set(record[2]);
        context.write(token,time);
      } catch (NumberFormatException e) {
        
      }
    }
  }
}
```

2、Combiner中的Reduce函数

> 说明：该部分主要是对单个Node数据记录进行处理，要统计该Node下通话次数和通话总时长，从而在Reduce中进行再次合并。

```java
public void reduce(Text token, Iterable<Text> values, Context context) throws IOException, InterruptedException {
  int count = 0;
  float alltime = 0;
  for (Text val : values) {
    count += 1;
    alltime += Float.parseFloat(val.toString());
  }
  result.set(String.valueOf(count)+" "+String.valueOf(alltime));
  //token是通话双方，result包含通话次数和通话总时长
  context.write(token, result);
}
```

3、Reduce模块

```java
public void reduce(Text token, Iterable<Text> values, Context context) throws IOException, InterruptedException {
  int count = 0;
  float alltime = 0;
  for (Text val : values) {
    //解析Combiner部分通话次数和通话总时长
    String[] split = val.toString().split(" ");
    count += Integer.parseInt(split[0]);
    alltime += Float.parseFloat(split[1]);
  }
  //再次进行封装
  result_value.set(String.valueOf(count) + " " + String.valueOf(alltime/count));
  context.write(token, result_value);
}
```

4、模块main函数中需要更改输出输出的类型即可，此外，在主函数中要设置键和值存入时的间隔方式改为空格

```java
job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
```

## 四、进行代码测试

参考"对示例程序WordCount的解析"部分。(测试数据给的过分干净了，可以自己尝试加入脏数据)

## 五、提交自动化测试机制

```shell
root@36f965b5aab0:~/hw2/hw2-check# ./myprepare
Deleted /hw2
Found 2 items
-rw-r--r--   1 root supergroup        259 2023-04-10 02:32 /hw2/part1-input/input_0
-rw-r--r--   1 root supergroup      36957 2023-04-10 02:32 /hw2/part1-input/input_1
root@36f965b5aab0:~/hw2/hw2-check# ./run-test-part1.pl ./score 0_xx_hw2.java 
./part1-result/result_0 has 5 lines
./part1-result/result_1 has 485 lines
------------------------------------------------------------
[Mon Apr 10 02:34:07 UTC 2023] 1 0_xx_hw2.java
------------------------------------------------------------
...
0_xx_hw2.java raw score: 2 / 2
```

