/* 0, 202228013329016, Liang Yulong */

#include <stdio.h>
#include <string.h>
#include <math.h>

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) SSSP##name

#define EPS 1e-6
#define MAX 1e6

int64_t m_v0_id = 0;

class VERTEX_CLASS_NAME(InputFormatter) : public InputFormatter
{
public:
    int64_t getVertexNum()
    {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex = n;
        return m_total_vertex;
    }
    int64_t getEdgeNum()
    {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge = n;
        return m_total_edge;
    }
    int getVertexValueSize()
    {
        m_n_value_size = sizeof(double);
        return m_n_value_size;
    }
    int getEdgeValueSize()
    {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }
    int getMessageValueSize()
    {
        m_m_value_size = sizeof(double);
        return m_m_value_size;
    }
    void loadGraph()
    {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;

        double value = 1;
        int outdegree = 0;

        const char *line = getEdgeLine();

        // Note: modify this if an edge weight is to be read
        //       modify the 'weight' variable

        sscanf(line, "%lld %lld %lf", &from, &to, &weight);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i)
        {
            line = getEdgeLine();

            // Note: modify this if an edge weight is to be read
            //       modify the 'weight' variable

            sscanf(line, "%lld %lld %lf", &from, &to, &weight);
            if (last_vertex != from)
            {
                addVertex(last_vertex, &value, outdegree);
                last_vertex = from;
                outdegree = 1;
            }
            else
            {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &value, outdegree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter) : public OutputFormatter
{
public:
    void writeResult()
    {
        int64_t vid;
        double value;
        char s[1024];

        for (ResultIterator r_iter; !r_iter.done(); r_iter.next())
        {
            r_iter.getIdValue(vid, &value);
            int n = sprintf(s, "%lld: %f\n", (unsigned long long)vid, value);
            writeNextResLine(s, n);
        }
    }
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator) : public Aggregator<double>
{
public:
    void init()
    {
        m_global = 0;
        m_local = 0;
    }
    void *getGlobal()
    {
        return &m_global;
    }
    void setGlobal(const void *p)
    {
        m_global = *(double *)p;
    }
    void *getLocal()
    {
        return &m_local;
    }
    void merge(const void *p)
    {
        m_global += *(double *)p;
    }
    void accumulate(const void *p)
    {
        m_local += *(double *)p;
    }
};

class VERTEX_CLASS_NAME() : public Vertex<double, double, double>
{
public:
    void compute(MessageIterator *pmsgs)
    {
        double val;
        // Set the initial value of each vertex at the super step to 0, the source point to 0, and the remaining points to MAX
        if (getSuperstep() == 0)
        {
            if (getVertexId() == m_v0_id)
            {
                val = 0;
            }
            else
            {
                val = MAX;
            }
        }
        else
        {
            // After the superstep is 2, the convergence is judged for each point, and the convergent point is changed to inactive
            if (getSuperstep() >= 2)
            {
                double global_val = *(double *)getAggrGlobal(0);
                if (global_val < EPS)
                {
                    voteToHalt();
                    return;
                }
            }
            val = getValue();
            // Each edge of the point is judged, and the node value is updated if a smaller length is passed in
            for (; !pmsgs->done(); pmsgs->next())
            {
                if (val > pmsgs->getValue())
                {
                    val = pmsgs->getValue();
                }
            }
            // Cumulative loss degree
            double acc = fabs(getValue() - val);
            accumulateAggr(0, &acc);
        }
        // Write the update node value to the node in the graph
        *mutableValue() = val;
        OutEdgeIterator out_edge_iter = getOutEdgeIterator();
        // Send a message to the outgoing edge
        for (; !out_edge_iter.done(); out_edge_iter.next())
        {
            // The message is "The shortest distance between you and the source point".
            double msg_value = out_edge_iter.getValue() + getValue();
            if (getValue() != MAX)
            {
                sendMessageTo(out_edge_iter.target(), msg_value);
            }
        }
    }
};

class VERTEX_CLASS_NAME(Graph) : public Graph
{
public:
    VERTEX_CLASS_NAME(Aggregator) * aggregator;

public:
    // argv[0]: SSSP.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    // argv[3]: <v0 id>
    void init(int argc, char *argv[])
    {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 4)
        {
            printf("Usage: %s <input path> <output path> <v0 id>\n", argv[0]);
            exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];
        m_v0_id = atoi(argv[3]);

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
    }

    void term()
    {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph *create_graph()
{
    Graph *pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph *pobject)
{
    delete (VERTEX_CLASS_NAME() *)(pobject->m_pver_base);
    delete (VERTEX_CLASS_NAME(OutputFormatter) *)(pobject->m_pout_formatter);
    delete (VERTEX_CLASS_NAME(InputFormatter) *)(pobject->m_pin_formatter);
    delete (VERTEX_CLASS_NAME(Graph) *)pobject;
}
