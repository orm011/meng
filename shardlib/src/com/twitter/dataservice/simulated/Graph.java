package com.twitter.dataservice.simulated;

import java.util.Iterator;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.simulated.parameters.WorkloadParams;

public interface Graph
{
   
    Iterator<Edge> graphIterator();    
    Iterator<Query> workloadIterator(WorkloadParams params);
    
}  
