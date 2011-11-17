package com.twitter.dataservice.simulated;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;


import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.parameters.GraphParameters;
import com.twitter.dataservice.simulated.parameters.SystemParameters;
import com.twitter.dataservice.simulated.parameters.WorkloadParams;

public class LatencyTrackingAPIServer implements IAPIServer
{
    IAPIServer measuredServer;
    static Logger logger = LoggerFactory.getLogger(LatencyTrackingAPIServer.class);

    enum Operation {
        FANOUT,
        EDGE,
        INTERSECTION,
    }

    //hypothesis: latency, throughput = f(operation_composition, graph_skew, query_skew, skew_correlation, max_degree, load, strategy, system_params)
    //system params: edge weight, number of data nodes, 
    //operation_compostion, load, graph_skew, query_skew skew_correlation, max_degree i control. load not quite yet, nor correlation.
    //general info that must go with each record so that I sleep well:
        //system parameters, ShardStrategy, graph parameters, workload parameters, timestamp    
    
    //TODO: timestamp format change in log4j (get rid of comma), 
    final List<Map.Entry<String, Object>> logPrefix = new LinkedList<Map.Entry<String, Object>>();
    final String prefix;
    
    void logFanout(long timeStart, long timeEnd,  Vertex arg, int answer){        
        logger.debug(prefix + "{},{},{},{},{},{},{},{},{},{}", new Object[]{"operation", Operation.FANOUT, "start", 
                timeStart, "end", timeEnd, "arg", arg.getId(), "answer", answer});
    }
    
    void logEdge(long timeStart, long timeEnd, Vertex left, Vertex right, boolean answer){
        logger.debug(prefix + "{},{},{},{},{},{},{},{},{},{},{},{}", new Object[]{"operation", Operation.EDGE, "start", timeStart, "end", timeEnd, 
                "left", left.getId(), "right", right.getId(), "answer", answer});
    }
    
    //NOTE: not Thread safe now
    public LatencyTrackingAPIServer(IAPIServer inner, WorkloadParams workloadParams, GraphParameters graphParams){
        measuredServer = inner;
        
        logPrefix.addAll(SystemParameters.fields());
        logPrefix.addAll(graphParams.fields());
        logPrefix.addAll(workloadParams.fields());
        
        StringBuilder sb = new StringBuilder();
        
        for (Iterator<Map.Entry<String, Object>> it = logPrefix.iterator(); it.hasNext(); ){
            Map.Entry<String, Object> entry = it.next();
            sb.append(entry.getKey());
            sb.append(",");
            sb.append(entry.getValue());
            sb.append(",");
        }
        
        prefix = sb.toString();
    }
    
    
    
    @Override
    public Edge getEdge(Vertex v, Vertex w)
    {
        long start = System.nanoTime();
        Edge ans = measuredServer.getEdge(v, w);
        logEdge(start, System.nanoTime(), v, w, ans != null);
        return ans;
    }

    @Override
    public Collection<Vertex> getFanout(Vertex v)
    {
        long start = System.nanoTime();
        Collection<Vertex> ans = measuredServer.getFanout(v);
        logFanout(start, System.nanoTime(), v, ans.size());
        return ans;
    }

    @Override
    public Collection<Vertex> getIntersection(Vertex v, Vertex w)
    {
        long start = System.nanoTime();
        Collection<Vertex> ans = getIntersection(v, w);
        return ans;
    }
    
    public List<Long> getLog(){
        return null;
    }

    @Override
    public void putEdge(Edge e)
    {
        measuredServer.putEdge(e);
    }
}