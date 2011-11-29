package com.twitter.dataservice.simulated;

import java.util.Iterator;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.simulated.parameters.WorkloadParameters;

public interface Graph
{
   /*
    * note, don't rename or move from .simulated
    */
    Iterator<Edge> graphIterator();    
    Iterator<Query> workloadIterator(WorkloadParameters params);
    
}  
