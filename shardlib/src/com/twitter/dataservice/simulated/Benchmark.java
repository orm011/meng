package com.twitter.dataservice.simulated;

import com.twitter.dataservice.shardutils.Vertex;

public class Benchmark {

  APIServer api;

  public Benchmark(APIServer api) {
    this.api = api;
  }


  public void run() {
     Vertex[] vertices = {new Vertex(0, 1), new Vertex(1, 1), new Vertex(2, 10), new Vertex(3, 10)};

     for (Vertex v: vertices){
       System.out.println(v.toString());
       api.getAllEdges(v);
     }
  }
}
