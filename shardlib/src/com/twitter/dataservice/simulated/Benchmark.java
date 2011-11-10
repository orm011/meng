package com.twitter.dataservice.simulated;

import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.commons.math.distribution.ExponentialDistributionImpl;
import org.apache.commons.math.distribution.ZipfDistribution;
import org.apache.commons.math.distribution.ZipfDistributionImpl;
import org.slf4j.LoggerFactory;


import com.jrefinery.chart.demo.JFreeChartDemo;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.BenchmarkData.Query;
import com.twitter.dataservice.simulated.BenchmarkData.WorkloadParams;

public class Benchmark {

    public static void main(String[] args) {        
      org.slf4j.Logger log = LoggerFactory.getLogger(Benchmark.class);      
      log.error("turun");
      if (log.isDebugEnabled()){
          System.out.println("haha");
      } else if (log.isErrorEnabled()){
          System.out.println("hehe");
      }
            
      log.debug("hello world");
      System.out.println("hello");
      
      
      //APIServer api = APIServer.apiWithGivenWorkNodes(givenN);
      //IAPIServer iapi = new LatencyTrackingAPIServer(api, 1);
      //SkewedDegreeGraph graph = new SkewedDegreeGraph(10, 1, 1);
      
      //iapi.getEdge(new Vertex(0), new Vertex(1));
      //load
      //Iterator<Edge> it = graph.graphIterator();
      //while (it.hasNext()){
//          api.putEdge(it.next());
//      }
//      
//      System.out.println("done creating graph");
//
//      //query
//      WorkloadParams params = (new WorkloadParams.Builder())
//      .numberOfQueries(10)    
//      .percentEdge(100)
//      .percentVertex(0)  
//      .skew(0.01)
//      .build();
//      
//      Iterator<Query> ot = graph.workloadIterator(params);
//      while (ot.hasNext()) ot.next().execute(api);
  }
}
