# 大数据系统HDFS+HashJoin+HBase实验记录

###### 梁宇龙	2023年3月17日于中国科学院大学

## 一、Docker容器初始化设置

### 1、创建容器

下载镜像：dingms/ucas-bdms-hw-u64-2019

创建一个新的容器：

```shell
docker run -itd <Image Id>
```

回显容器编号:\<CONTAINER ID>，此时容器已经完成创建。

进入容器中启动bash

```shell
docker exec -it <CONTAINER ID> /bin/bash
```

### 2、容器内初始化设置

> PS:此模块在容器停止后每次启动都要执行。

重新加载SSH模块：

```shell
root@<CONTAINER ID>:/# service ssh stop
 * Stopping OpenBSD Secure Shell server sshd                                    
 start-stop-daemon: warning: failed to kill 4008: No such process
                                                                         [ OK ]
root@<CONTAINER ID>:/# service ssh start
 * Starting OpenBSD Secure Shell server sshd                             [ OK ] 
```

启动HDFS模块：

```shell
root@<CONTAINER ID>:/# start-dfs.sh
Starting namenodes on [localhost]
localhost: starting namenode, logging to /home/bdms/setup/hadoop-2.9.2/logs/hadoop-root-namenode-2ce55076d336.out
localhost: starting datanode, logging to /home/bdms/setup/hadoop-2.9.2/logs/hadoop-root-datanode-2ce55076d336.out
Starting secondary namenodes [0.0.0.0]
0.0.0.0: starting secondarynamenode, logging to /home/bdms/setup/hadoop-2.9.2/logs/hadoop-root-secondarynamenode-2ce55076d336.out
```

确认HDFS可以使用：

```shell
root@<CONTAINER ID>:/# jps
310 DataNode
487 SecondaryNameNode
175 NameNode
655 Jps
root@<CONTAINER ID>:/# hadoop fs -ls /
Found 1 items
drwxr-xr-x   - root supergroup          0 2019-03-15 10:03 /hbase
```

## 二、将数据传入HDFS中

实验一数据路径存放在/home/bdms/homework/hw1/input中

```shell
root@<CONTAINER ID>:/# cd home/bdms/homework/hw1/input                 
root@<CONTAINER ID>:/home/bdms/homework/hw1/input# ls
customer.tbl  nation.tbl  part.tbl      region.tbl
lineitem.tbl  orders.tbl  partsupp.tbl  supplier.tbl
```

在hadoop中创建目录hw1

```shell
root@<CONTAINER ID>:/home/bdms/homework/hw1/input# hadoop fs -mkdir /hw1
```

将实验数据存入HDFS中，并验证是否存入成功

```shell
root@<CONTAINER ID>:/home/bdms/homework/hw1/input# hadoop fs -put *.tbl /hw1      
root@<CONTAINER ID>:/home/bdms/homework/hw1/input# hadoop fs -ls -R /hw1
-rw-r--r--   1 root supergroup     240990 2023-03-16 10:17 /hw1/customer.tbl
-rw-r--r--   1 root supergroup    7264250 2023-03-16 10:17 /hw1/lineitem.tbl
-rw-r--r--   1 root supergroup       2224 2023-03-16 10:17 /hw1/nation.tbl
-rw-r--r--   1 root supergroup    1659137 2023-03-16 10:17 /hw1/orders.tbl
-rw-r--r--   1 root supergroup     238074 2023-03-16 10:17 /hw1/part.tbl
-rw-r--r--   1 root supergroup    1161705 2023-03-16 10:17 /hw1/partsupp.tbl
-rw-r--r--   1 root supergroup        389 2023-03-16 10:17 /hw1/region.tbl
-rw-r--r--   1 root supergroup      13795 2023-03-16 10:17 /hw1/supplier.tbl
```

使用测试代码HDFSTest.java尝试打开某一个数据

```shell
root@<CONTAINER ID>:/home/bdms/homework/hw1/input# cd ../example/
root@<CONTAINER ID>:/home/bdms/homework/hw1/example# javac HDFSTest.java 
root@<CONTAINER ID>:/home/bdms/homework/hw1/example# java HDFSTest hdfs://localhost:9000/hw1/customer.tbl
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/home/bdms/setup/hadoop-2.9.2/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/home/bdms/setup/hbase-0.98.11-hadoop2/lib/slf4j-log4j12-1.6.4.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
23/03/16 10:21:23 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
1|Customer#000000001|IVhzIApeRb ot,c,E|15|25-989-741-2988|711.56|BUILDING|to the even, regular platelets. regular, ironic epitaphs nag e|
2|Customer#000000002|XSTf4,NCwDVaWNe6tEgvwfmRchLXak|13|23-768-687-3665|121.65|AUTOMOBILE|l accounts. blithely ironic theodolites integrate boldly: caref|
......
1500|Customer#000001500|4zaoUzuWUTNFiNPbmu43|5|15-200-872-4790|6910.79|MACHINERY|s boost blithely above the fluffily ironic dolphins! ironic accounts|
```

## 三、尝试解析位置参数

> R=<file 1> S=<file 2> join:R2=S3 res:R4,S5

对位置参数args解析目标变量如下：

```java
args[0]:R=/hw1/a.tbl	--------> /*目标：File_R_Uri = "/hw1/a.tbl"*/
args[1]:S=/hw1/b.tbl	--------> /*目标：File_S_Uri = "/hw1/a.tbl"*/
args[2]:join:R2=S3		--------> /*目标：JoinKeyForR = 2,JoinKeyForS = 3*/
args[3]:res:R4,S5			--------> /*目标：ResList = [R4,S5]*/
```

进行参数处理如下：

```Java
public void ParamProcessing(String[] args){
  File_R_Uri = args[0].split("=")[1];
  File_S_Uri = args[1].split("=")[1];
  JoinKeyForR = Integer.parseInt(args[2].split(":")[1].split("=")[0].substring(1));
  JoinKeyForS = Integer.parseInt(args[2].split(":")[1].split("=")[1].substring(1));
  resList = Arrays.asList(args[3].split(":")[1].split(","));
}
```

## 四、读取数据并存储在程序中

基本使用baseline代码即可，最终将数据读取为List\<String>的形式。

```java
public List<String> readHDFSTable(String file_Uri) throws IOException, URISyntaxException {
  Configuration conf = new Configuration();
  FileSystem fs = FileSystem.get(URI.create(HDFSUri + file_Uri), conf);
  Path path = new Path(HDFSUri + file_Uri);
  FSDataInputStream in_stream = fs.open(path);
  BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
  String line = null;
  List<String> strings = new ArrayList<>();
  while ((line = in.readLine()) != null) {
    strings.add(line);
  }
  in.close();
  fs.close();
  return strings;
}
```

而后将List\<string>变为hashtable

```java
public Hashtable<String,List<String[]>> HDFSToHashTable(List<String> lines){
  Hashtable<String, List<String[]>> stringListHashtable = new Hashtable<String, List<String[]>>();
  for (String line : lines){
    String[] split = line.split("\\|");
    stringListHashtable.put(split[joinKeyForR], Collections.singletonList(split));
  }
  return stringListHashtable; 
}
```

使用customer.tbl试运行的中间结果如下；

```shell
root@<CONTAINER ID>:~# java Hw1Grp0 R=/hw1/customer.tbl S=/hw1/customer.tbl join:R2=S3 res:R4,S5
{
  kbYrf d uR=[[Ljava.lang.String;@2a640157], 
	naLuK8XKUP72msE0e=[[Ljava.lang.String;@52851b44], 
  e53JADEeGvM1ikhN7aa=[[Ljava.lang.String;@584f54e6], 
  5J941XxxkE=[[Ljava.lang.String;@5d8bafa9],
  ...,
  GLZCUQrtiNTrPKdK 0O86ZF=[[Ljava.lang.String;@2755d705]
}
```

这里可能会出现的问题：如果需要做hashjoin的key存在一个key对应多个value的情况该如何保存？

解决方法，通过对key判定是否存值分类讨论。

```java
public Hashtable<String,List<String[]>> HDFSToHashTable(List<String> lines){
  Hashtable<String, List<String[]>> stringListHashtable = new Hashtable<String, List<String[]>>();
  for (String line : lines){
    String[] split = line.split("\\|");
    /*使用分类讨论方式代替直接寻值覆盖*/
    if (stringListHashtable.get(split[joinKeyForR])!=null){
      List<String[]> strings = stringListHashtable.get(split[joinKeyForR]);
      strings.add(split);
      stringListHashtable.put(split[joinKeyForR],strings);
    }
    else {
      ArrayList<String[]> strings = new ArrayList<>();
      strings.add(split);
      stringListHashtable.put(split[joinKeyForR],strings);
    }
  }
  return stringListHashtable;
}
```

此时结果：

```shell
root@<CONTAINER ID>:~# java Hw1Grp0 R=/hw1/customer.tbl S=/hw1/customer.tbl join:R3=S3 res:R4,S5
{
  19=[[Ljava.lang.String;@2a640157,..., [Ljava.lang.String;@10ded6a9],
	18=[[Ljava.lang.String;@c5dc4a2,..., [Ljava.lang.String;@5911e990], 
  17=[[Ljava.lang.String;@31000e60,..., [Ljava.lang.String;@49872d67], 
  ...,
  20=[[Ljava.lang.String;@770d0ea6,..., [Ljava.lang.String;@2755d705]
}
/*此时，一个key中可对应保存多个value值*/
```

## 五、HashJoin连接

使用已经对表R生成的HashTable和已经完成表S分词的String[]进行hashJoin连接

```Java
	List<String> stringsForR = hw1Grp0.readHDFSTable(file_R_Uri);
	Hashtable<String, List<String[]>> stringListHashtable = hw1Grp0.HDFSToHashTable(stringsForR);
	List<String> stringsForS = hw1Grp0.readHDFSTable(file_S_Uri);
	hw1Grp0.hashJoin(stringListHashtable,stringsForS);
```

如何实现：

```Java
public void hashJoin(Hashtable<String, List<String[]>> R,List<String> strings_S){
  //创建投影集合，确保在HashJoin后可以准确投影
  ArrayList<Integer> projectionForR = new ArrayList<>();
  ArrayList<Integer> projectionForS = new ArrayList<>();
  for (String res : resList){
    if (res.charAt(0) == 'R'){
      projectionForR.add(Integer.valueOf(res.substring(1)));
    }
    if (res.charAt(0) == 'S'){
      projectionForS.add(Integer.valueOf(res.substring(1)));
    }
  }
  //对表S中每一条记录进行Hash查询，找到可以进行HashJoin的表R中的所有记录，对每一条记录进行hashJoin
  for (String line : strings_S){
    String[] split = line.split("\\|");
    if (R.get(split[joinKeyForS])!=null){
      List<String[]> strings = R.get(split[joinKeyForS]);
      for (String[] record : strings){
        hashJoinForOneRecord(record,split,projectionForR,projectionForS);
      }
    }
  }
}
```

使用数据集进行结果验证：

```shell
root@<CONTAINER ID>:~# java Hw1Grp0 R=/hw1/nation.tbl S=/hw1/customer.tbl join:R2=S3 res:R1,S4,S5
-join key=1,R1=ARGENTINA,S4=11-719-748-3364,S5=7498.12
-join key=1,R1=BRAZIL,S4=11-719-748-3364,S5=7498.12
-join key=1,R1=CANADA,S4=11-719-748-3364,S5=7498.12
-join key=1,R1=PERU,S4=11-719-748-3364,S5=7498.12
-join key=1,R1=UNITED STATES,S4=11-719-748-3364,S5=7498.12
-join key=4,R1=EGYPT,S4=14-128-190-5944,S5=2866.83
...
-join key=3,R1=UNITED KINGDOM,S4=13-802-978-9538,S5=-496.49
-join key=3,R1=FRANCE,S4=13-273-527-9609,S5=9128.69
-join key=3,R1=GERMANY,S4=13-273-527-9609,S5=9128.69
-join key=3,R1=ROMANIA,S4=13-273-527-9609,S5=9128.69
-join key=3,R1=RUSSIA,S4=13-273-527-9609,S5=9128.69
-join key=3,R1=UNITED KINGDOM,S4=13-273-527-9609,S5=9128.69
total 1615
```

