package com.twitter.dataservice.simulated;

import java.util.Iterator;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Pair;
import com.twitter.dataservice.parameters.WorkloadParameters;

public interface Graph
{
   /*
    * note, don't rename or move from .simulated
    */
    Iterator<Edge> graphIterator();
    Iterator<Pair<Integer, int[]>> fanoutIterator();
    Iterator<Query> workloadIterator(WorkloadParameters params);
    
}  
