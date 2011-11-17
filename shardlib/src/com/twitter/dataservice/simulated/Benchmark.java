package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;


import com.twitter.dataservice.remotes.ICompleteWorkNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.simulated.parameters.GraphParameters;
import com.twitter.dataservice.simulated.parameters.WorkloadParams;

public class Benchmark {
    
    static Logger logger;
    
    static {
        PropertyConfigurator.configure("/Users/oscarm/workspace/oscarmeng/shardlib/log4j.properties");
        logger = org.slf4j.LoggerFactory.getLogger(Benchmark.class);
    }
    
    public static void main(String[] args) {
        
      ICompleteWorkNode[] nodes;

      try
        {
            nodes = new ICompleteWorkNode[]{new CounterBackedWorkNode(), new CounterBackedWorkNode()};
        } catch (RemoteException e)
        {
            throw new RuntimeException(e);
        }
      
      IAPIServer apiServer = APIServer.apiWithGivenWorkNodes(Arrays.asList(nodes));
      
      GraphParameters graphParams = new GraphParameters();
      
      Graph graph = graphParams
      .degreeSkew(1)
      .maxDegree(10)
      .numberEdges(10)
      .numberVertices(5)
      .build();
      
      WorkloadParams workloadParams = (new WorkloadParams.Builder())
      .numberOfQueries(10)    
      .percentEdge(50)
      .percentVertex(50)  
      .skew(1)
      .build();

      IAPIServer api = new LatencyTrackingAPIServer(apiServer, workloadParams, graphParams);
      
      Iterator<Edge> it = graph.graphIterator();
      while (it.hasNext()){
          apiServer.putEdge(it.next());
      }
      
      System.out.println("done loading graph");
      
      Iterator<Query> ot = graph.workloadIterator(workloadParams);
            
      while (ot.hasNext()) ot.next().execute(api);
    }
    
}
