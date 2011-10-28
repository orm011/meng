package com.twitter.dataservice.simulated;

import java.nio.channels.UnsupportedAddressTypeException;
import java.util.Random;

import org.apache.commons.math.distribution.ExponentialDistributionImpl;
import org.apache.commons.math.distribution.ZipfDistribution;
import org.apache.commons.math.distribution.ZipfDistributionImpl;

import com.twitter.dataservice.shardutils.Vertex;

class Benchmark {

  APIServer api;
  
  //
  static ExponentialDistributionImpl expy = new ExponentialDistributionImpl(2);
  static ZipfDistributionImpl zipfy = new ZipfDistributionImpl(1, 1.5);

  public Benchmark(APIServer api) {
    this.api = api;
  } 
  
  public void run() {
      //would generate graph, put into nodes
//      Object graph;
//     
//      while (graph.edges.hasNext()){
//          api.putEdge(graph.edges.next());//
//      }
//
//      for (Object query: benchmarkIterator)
//              query.getCalled(api);
//      

            
     Vertex[] vertices = {new Vertex(0, 1), new Vertex(1, 1), new Vertex(2, 10), new Vertex(3, 10)};

     for (Vertex v: vertices){
       System.out.println(v.toString());
       api.getAllEdges(v);
     }
  }
}
