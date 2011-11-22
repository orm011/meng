package com.twitter.dataservice.simulated;

import com.twitter.dataservice.shardutils.Vertex;

/*
 * External scripts depend on the interface name, don't change
 */
public interface MetricsCollector
{
    void begin();
    
    void logFanout(long timeStart, long timeEnd,  Vertex arg, int answer);
    
    void logEdge(long timeStart, long timeEnd, Vertex left, Vertex right, boolean answer);
    
    void logIntersection(long timeStart, long timeEnd, Vertex arg1, Vertex arg2, int answer);
    
    void finish();

}