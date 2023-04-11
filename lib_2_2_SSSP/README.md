# 大数据运算系统SSSP实验记录

###### 梁宇龙	2023年4月11日于中国科学院大学

## 一、容器环境初始化

```shell
root@<container id>:~# cd ../home/bdms/setup/GraphLite-0.20/
root@<container id>:/home/bdms/setup/GraphLite-0.20# source bin/setenv
root@<container id>:/home/bdms/setup/GraphLite-0.20# cd engine
root@<container id>:/home/bdms/setup/GraphLite-0.20/engine# make
cp graphlite ../bin
root@<container id>:/home/bdms/setup/GraphLite-0.20/engine# cd ../example/
root@<container id>:/home/bdms/setup/GraphLite-0.20/example# make
make: Nothing to be done for 'all'.
root@<container id>:/home/bdms/setup/GraphLite-0.20/example# cd ../
root@<container id>:/home/bdms/setup/GraphLite-0.20# service ssh start
 * Starting OpenBSD Secure Shell server sshd                             [ OK ] 
```

## 二、对PageRank示例代码分析

```C++
//该部分是实现PageRank算法的核心代码
class VERTEX_CLASS_NAME(): public Vertex <double, double, double> {
public:
  void compute(MessageIterator* pmsgs) {
    double val;
    //当超步为0时对整个图进行初始化。
    if (getSuperstep() == 0) {
      val= 1.0;
    }
    else {
      //当超步大于等于2，即已完成一次运算时，可以判断是否满足收敛条件结束运行
      if (getSuperstep() >= 2) {
        double global_val = * (double *)getAggrGlobal(0);
        if (global_val < EPS) {
          voteToHalt(); return;
        }
      }
      //不满足收敛条件时，对其进行PageRank计算
      double sum = 0;
      for ( ; ! pmsgs->done(); pmsgs->next() ) {
        sum += pmsgs->getValue();
      }
      val = 0.15 + 0.85 * sum;
      double acc = fabs(getValue() - val);
      accumulateAggr(0, &acc);
    }
    * mutableValue() = val;
    const int64_t n = getOutEdgeIterator().size();
    sendMessageToAllNeighbors(val / n);
  }
};
```

## 三、编写SSSP代码

1、对示例代码文件输入部分进行修改，确保边的权重存储在图中

```c++
class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
  ...
  void loadGraph() {
    ...
    const char *line= getEdgeLine();
    //将文件中的边值存储在weight中
    sscanf(line, "%lld %lld %lf", &from, &to, &weight);
    addEdge(from, to, &weight);
    ...
    for (int64_t i = 1; i < m_total_edge; ++i) {
      line= getEdgeLine();
      //扫描行时将边值存储在weight中
      sscanf(line, "%lld %lld %lf", &from, &to, &weight);
      ...
    }
};
```

2、修改位置变量个数并提取出位置变量3(起始点)

```c++
class VERTEX_CLASS_NAME(Graph): public Graph {
public:
  void init(int argc, char* argv[]) {
    ...
    //位置参数除输入文件、输出路径外，还有源点
    if (argc < 4) {
      printf ("Usage: %s <input path> <output path> <v0 id>\n", argv[0]);
      exit(1);
    }
    m_pin_path = argv[1];
    m_pout_path = argv[2];
    //源点要以整型进行存储
    m_v0_id = atoi(argv[3]);
  }
  ...
};
```

3、编写SSSP核心代码

```c++
class VERTEX_CLASS_NAME(): public Vertex <double, double, double> {
public:
  void compute(MessageIterator* pmsgs) {
    double val;
    if (getSuperstep() == 0) {
      //在超步为0设置各顶点初始值,源点为0,其余点为MAX
      if (getVertexId()==m_v0_id){
        val = 0;
      }
      else{
        val = MAX;
      }
    }
    else {
      //在超步为2之后,对每个点判断收敛性,对收敛的点将其变为不活跃
      if (getSuperstep() >= 2) {
        double global_val = * (double *)getAggrGlobal(0);
        if (global_val < EPS) {
          voteToHalt(); return;
        }
      }
      val = getValue();
      //对点各入度边进行判定,有更小的长度传入则对节点值进行更新
      for ( ; ! pmsgs->done(); pmsgs->next() ) {
        if(val > pmsgs->getValue()){
          val = pmsgs->getValue();
        }
      }
      //累计损失度
      double acc = fabs(getValue() - val);
      accumulateAggr(0, &acc);
    }
    //将更新节点值写入图中节点
    * mutableValue() = val;
    OutEdgeIterator out_edge_iter = getOutEdgeIterator();
    //向出度边发送消息
    for(; !out_edge_iter.done(); out_edge_iter.next()){
      //消息为"你经过我到源点的最短距离"
      double msg_value = out_edge_iter.getValue() + getValue();
      //对非初始化的节点不更新
      if(getValue() != MAX){
        sendMessageTo(out_edge_iter.target(),msg_value);
      }
    }
  }
};
```
