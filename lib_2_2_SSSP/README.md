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

