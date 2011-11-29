package com.twitter.dataservice.simulated;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;

import org.apache.log4j.FileAppender;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.dataservice.remotes.ICompleteWorkNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.parameters.GraphParameters;
import com.twitter.dataservice.simulated.parameters.SystemParameters;
import com.twitter.dataservice.simulated.parameters.WorkloadParameters;

public class Benchmark {
    
    static org.slf4j.Logger logger;
    
    static {
        PropertyConfigurator.configure("/Users/oscarm/workspace/oscarmeng/shardlib/log4j.properties");
        logger = org.slf4j.LoggerFactory.getLogger(Benchmark.class);
    }
    
    public static void main(String[] args) {

//        //0.001, 0.1, 0.3 ... 5
//        double max = 6.0;
//        double min = 0.1;
//        double step = 0.1;
//        int num = (int) ((max - min)/step);
//        double[] skews = new double[num];
//        
//        for (int i = 0; i < num; i++){
//            skews[i] = min + i*step;    
//            runBenchmark(skews[i]);
//        }
        runBenchmark(100, 1000000);
        //TODO: change this to take inputs (more) from elsewhere
        System.exit(0);
    }
        
    public static void runBenchmark(double skew, int edgesPerNode){
      ICompleteWorkNode[] nodes;

      try {
            nodes = new ICompleteWorkNode[SystemParameters.instance().workNodes];
            
            for (int i = 0; i < SystemParameters.instance().workNodes; i++){
                nodes[i] = new CounterBackedWorkNode();
            }
      } catch (RemoteException e) {
            throw new RuntimeException(e);
      }
      
      IAPIServer apiServer = APIServer.apiWithGivenWorkNodes(Arrays.asList(nodes)); 
      
      int VERTICES  = 1000;

      //Note: making maxDegree 100k made the generation part take very long.
      GraphParameters graphParams = new GraphParameters();
      Graph graph = graphParams
      .degreeSkew(skew)
      .numberVertices(VERTICES)
      .degreeBoundAndTargetAvg(VERTICES, edgesPerNode)
      .build();      
      
      int QUERIES = 1000;
      
      WorkloadParameters workloadParams = (new WorkloadParameters.Builder())
      .numberOfQueries(QUERIES)    
      .percentEdge(0)
      .percentVertex(100)  
      .skew(0.001)
      .build();

      //MetricsCollector omc = new LatencyTrackingAPIServer.LoggerCollector(graphParams, workloadParams);
      MetricsCollector omc = new LatencyTrackingAPIServer.InMemoryCollector(graphParams, workloadParams);
      IAPIServer api = new LatencyTrackingAPIServer(apiServer, omc);      
      
      Iterator<Edge> it = graph.graphIterator();

      omc.begin();
      
      //log degrees for auditing later
      Logger graphLogger = LoggerFactory.getLogger(Graph.class);      
      for (int i = 0; i < graphParams.getNumberVertices();  ++i) graphLogger.debug("{}", ((SkewedDegreeGraph)graph).getDegree(i));
      System.out.println("\n");
      System.out.println("done logging graph");
      
      int i = 0;
      while (it.hasNext()){
          apiServer.putEdge(it.next());
          i++;
      }
     
      System.out.println("done loading graph");
      
      Iterator<Query> ot = graph.workloadIterator(workloadParams);     
      while (ot.hasNext()) {
          ot.next().execute(api);   
      }
      
      omc.finish();
      System.out.println("done with bench");

      ProcessBuilder degreePlotting = new ProcessBuilder(
              new String[]{
              "./plot.sh", 
              "--degree",  
              "foo", //placeholder, will delete soon
              String.valueOf(graphParams.getUpperDegreeBound()*1.2), //maxx
              String.valueOf(graphParams.getNumberVertices()/4), //maxy
              TimestampNameFileAppender.getLogName()})
      .directory(new File("/Users/oscarm/workspace/oscarmeng/shardlib/logs"));
            
      ProcessBuilder latencyPlotting = new ProcessBuilder(
              new String[]{
              "./plot.sh", 
              "--latency", 
              "foo", 
              "100", 
              String.valueOf(QUERIES/2), 
              TimestampNameFileAppender.getLogName()})
      .directory(new File("/Users/oscarm/workspace/oscarmeng/shardlib/logs/"));
      
      System.out.println(degreePlotting.command());
      
      for (String s: latencyPlotting.command()) {
          System.out.printf("%s ", s);
      }
      System.out.println("\n");
      
      try
      {
//          Process p = degreePlotting.start();
//          p.waitFor();
//          System.out.println("done with degree plot");
          Process q = latencyPlotting.start();
          q.waitFor();
          
          System.out.println("done with latency plot");
          q.getOutputStream();
      } catch (IOException e)
      {
          e.printStackTrace();
      } catch (InterruptedException e)
      {
          e.printStackTrace();
      }

    }   
}