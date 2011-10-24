package com.twitter.dataservice.simulated;

import java.util.Random;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.ExponentialDistributionImpl;
import org.apache.commons.math.distribution.ZipfDistributionImpl;

import com.twitter.dataservice.shardutils.Vertex;

public class Benchmark {

  APIServer api;
  static ExponentialDistributionImpl expy = new ExponentialDistributionImpl(2);
  static ZipfDistributionImpl zipfy = new ZipfDistributionImpl(100, 1.5);

  public Benchmark(APIServer api) {
    this.api = api;
  }

  
  //exponential
  static public int nextInt(){
      try
    {
        return zipfy.sample();
    } catch (MathException e)
    {
        // TODO Auto-generated catch block
        e.printStackTrace();
        return 0;
    }
  }
  
  
  public void run() {


     Vertex[] vertices = {new Vertex(0, 1), new Vertex(1, 1), new Vertex(2, 10), new Vertex(3, 10)};

     for (Vertex v: vertices){
       System.out.println(v.toString());
       api.getAllEdges(v);
     }
  }
}
