package com.twitter.dataservice.simulated;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.NotImplementedException;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.parameters.GraphParameters;
import com.twitter.dataservice.parameters.WorkloadParameters;

public class LatencyTrackingAPIServer implements IAPIServer
{
    IAPIServer measuredServer;
    MetricsCollector collector;

    enum Operation {
        FANOUT,
        EDGE,
        INTERSECTION,
    };

    //hypothesis: latency, throughput = f(operation_composition, graph_skew, query_skew, skew_correlation, max_degree, load, strategy, system_params)
    //system params: edge weight, number of data nodes, 
    //operation_compostion, load, graph_skew, query_skew skew_correlation, max_degree i control. load not quite yet, nor correlation.
    //general info that must go with each record so that I sleep well:
        //system parameters, graph parameters, workload parameters, timestamp    
        //TODO: add sharding Strategy param
        //TODO: check if logging is affecting performance. done: it is. goes from 400 /s to 350 /s.
    static class LoggerCollector extends BasicCollector implements MetricsCollector {
        
        public LoggerCollector(GraphParameters graphParams, WorkloadParameters workloadParams)
        {
            super(graphParams, workloadParams);
        }

        //TODO: use the abstractParameters class for this?
        public void logFanout(long timeStart, long timeEnd,  Vertex arg, int answer){
            System.out.println("fanout log");
            Object[] contents = new Object[]{
                    "operation", Operation.FANOUT, 
                    "start", timeStart, 
                    "end", timeEnd, 
                    "arg", arg.getId(), 
                    "answer", answer
                    };
            
            logger.debug("{} {} {} {} {} {} {} {} {} {}", contents);
        }
    
        public void logEdge(long timeStart, long timeEnd, Vertex left, Vertex right, boolean answer){
            Object[] contents = new Object[]{
                    "operation", Operation.EDGE,
                    "start", timeStart,
                    "end", timeEnd, 
                    "leftarg", left.getId(),
                    "rightarg", right.getId(),
                    "answer", answer
                    };
            logger.debug("{} {} {} {} {} {} {} {} {} {} {} {}", contents);
        } 
        
        @Override
        public void finish()
        {
            return;
        }
        
        @Override
        public void logIntersection(long timeStart, long timeEnd, Vertex arg1, Vertex arg2, int answer)
        {
            // TODO Auto-generated method stub
            throw new NotImplementedException();
        }
    };
    
    public static class InMemoryCollector extends BasicCollector implements MetricsCollector {        
        AtomicInteger endsPos = new AtomicInteger(0);
        long[] ends;
        
        AtomicInteger latenciesPos = new AtomicInteger(0);
        long[] latencies;
        
        public InMemoryCollector(GraphParameters graphParams, WorkloadParameters workloadParams)
        {
            super(graphParams, workloadParams);            
            ends = new long[workloadParams.getNumberOfQueries()];
            latencies = new long[workloadParams.getNumberOfQueries()];
        }

        @Override
        public void finish()
        {   
            Logger logger = LoggerFactory.getLogger(MetricsCollector.class);   
            for (long l: latencies) logger.info("{}", l);
        }

        @Override
        public void logEdge(long timeStart, long timeEnd, Vertex left, Vertex right, boolean answer)
        {
            int pos = endsPos.getAndIncrement();
            ends[pos] = timeEnd;
            latencies[pos] = timeEnd - timeStart;
            latencies[pos] /= 1000;
        }

        @Override
        public void logFanout(long timeStart, long timeEnd, Vertex arg, int answer)
        {
            int pos = endsPos.getAndIncrement();
            ends[pos] = timeEnd;
            latencies[pos] = timeEnd - timeStart;
            latencies[pos] /= 1000;
        }

        @Override
        public void logIntersection(long timeStart, long timeEnd, Vertex arg1, Vertex arg2, int answer)
        {
            int pos = endsPos.getAndIncrement();
            ends[pos] = timeEnd;
            latencies[pos] = timeEnd - timeStart;
            latencies[pos] /= 1000;
        }
        
    };

    //NOTE: not Thread safe
    public LatencyTrackingAPIServer(IAPIServer inner, MetricsCollector mc){
        measuredServer = inner;
        collector = mc;
    }
        
    @Override
    public Edge getEdge(Vertex v, Vertex w)
    {
        long start = System.nanoTime();
        //System.out.println("getEdge: " + v + w);
        Edge ans = measuredServer.getEdge(v, w);
        //System.out.println("answer: " + ans);
        collector.logEdge(start, System.nanoTime(), v, w, ans != null);
        return ans;
    }

    @Override
    public List<Vertex> getFanout(Vertex v, int pageSize, int offset)
    {
        long start = System.nanoTime();
        //System.out.printf("getFanout: %s, %d, %d\n", v, pageSize, offset);
        List<Vertex> ans = measuredServer.getFanout(v, pageSize, offset);
        //System.out.println("fanout ans: " + ans);
        collector.logFanout(start, System.nanoTime(), v, ans.size());
        return ans;
    }

    @Override
    public List<Vertex> getIntersection(Vertex v, Vertex w, int pageSize, int offset)
    {
        long start = System.nanoTime();
        //System.out.printf("getIntersection: %s, %s, %d, %d\n", v, w, pageSize, offset);
        List<Vertex> ans = measuredServer.getIntersection(v, w, pageSize, offset);
        //System.out.printf("intersectionAnswer: %s\n", ans);
        collector.logIntersection(start, System.nanoTime(), v, w, ans.size());
        return ans;
    }
    

    @Override
    public void putEdge(Edge e)
    {
        measuredServer.putEdge(e);
    }
    
    @Override
    public void putFanout(int vertexid, int[] fanouts){
        measuredServer.putFanout(vertexid, fanouts);
    }

	@Override
	public Collection<Stats> stat() {
		return measuredServer.stat();
	}
}