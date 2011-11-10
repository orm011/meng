package com.twitter.dataservice.simulated;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;


import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public class LatencyTrackingAPIServer implements IAPIServer
{
    IAPIServer measuredServer;
    Logger logger = LoggerFactory.getLogger(LatencyTrackingAPIServer.class);
    Marker perf = MarkerFactory.getMarker("PERF");
    
    
    //NOTE: not Thread safe now
    public LatencyTrackingAPIServer(IAPIServer inner, int workloadSize){
        measuredServer = inner;
    }
    
    @Override
    public Edge getEdge(Vertex v, Vertex w)
    {
        long start = System.nanoTime();
        Edge ans = measuredServer.getEdge(v, w);
        logger.debug(perf, "{}", System.nanoTime() - start);
        return ans;
    }

    @Override
    public Collection<Vertex> getFanout(Vertex v)
    {
        long start = System.nanoTime();
        Collection<Vertex> ans = getFanout(v);
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
    
//    public void latencyHistogram(){       
//    }
//    
//    public long latency(int percentile){
//        return null;
//    }
}