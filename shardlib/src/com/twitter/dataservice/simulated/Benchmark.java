package com.twitter.dataservice.simulated;

import java.util.Iterator;
import java.util.Random;

import org.apache.commons.math.distribution.ExponentialDistributionImpl;
import org.apache.commons.math.distribution.ZipfDistribution;
import org.apache.commons.math.distribution.ZipfDistributionImpl;

import com.jrefinery.chart.demo.JFreeChartDemo;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.BenchmarkData.Query;
import com.twitter.dataservice.simulated.BenchmarkData.WorkloadParams;

public class Benchmark {

  public static void main(String[] args) {
      APIServer api = APIServer.apiWithRemoteWorkNodes(args);
      SkewedDegreeGraph graph = new SkewedDegreeGraph(10, 1, 1);
      //load
      Iterator<Edge> it = graph.graphIterator();
      while (it.hasNext()){
          api.putEdge(it.next());
      }
      
      System.out.println("done creating graph");

      //query
      WorkloadParams params = (new WorkloadParams.Builder())
      .numberOfQueries(10)
      .percentEdge(100)
      .percentVertex(0)  
      .skew(0.01)
      .build();
      
      Iterator<Query> ot = graph.workloadIterator(params);
      while (ot.hasNext()) ot.next().execute(api);
  }
}
