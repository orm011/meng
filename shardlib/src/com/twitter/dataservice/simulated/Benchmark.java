package com.twitter.dataservice.simulated;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.simulated.parameters.GraphParameters;
import com.twitter.dataservice.simulated.parameters.SystemParameters;
import com.twitter.dataservice.simulated.parameters.WorkloadParameters;

public class Benchmark {
//    # (Milestone 1.1) Finish framework (< 1 unit)
//
//    1. parallelizing shard requests within an api request
//    2. figuring out discrepancy in latency vs degree graph (ensure enough "work" is being done)

    static org.slf4j.Logger logger;
    
    static {
    }
    
    public static void main(String[] args) {
        //TODO: read some logname marker string from the config as well. will help in selecting all the logs 
        // for analysis. 
        //TODO: make plot optional 
        PropertyConfigurator.configure("log4j.properties");
        logger = org.slf4j.LoggerFactory.getLogger(Benchmark.class);
        

        Properties prop = new Properties();
        
        try
        {
            prop.load(new FileInputStream("Benchmark.properties"));
        } catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
               
        GraphParameters gp = new GraphParameters.Builder()
        .degreeBoundAndTargetAvg(
                Integer.parseInt(prop.getProperty(GraphParameters.MAXDEGREE)), 
                Integer.parseInt(prop.getProperty(GraphParameters.AVERAGE_DEGREE)))
        .degreeSkew(
                Double.parseDouble(prop.getProperty(GraphParameters.SKEW_PARAMETER)))
        .numberVertices(
                Integer.parseInt(prop.getProperty(GraphParameters.NUMBER_VERTICES))).
        build();
        
        
        WorkloadParameters wp = new WorkloadParameters.Builder()
        .numberOfQueries(
                Integer.parseInt(prop.getProperty(WorkloadParameters.NUMBER_OF_QUERIES)))
        .percentEdge(
                Integer.parseInt(prop.getProperty(WorkloadParameters.PERCENT_EDGE_QUERIES)))
        .percentVertex(
                Integer.parseInt(prop.getProperty(WorkloadParameters.PERCENT_FANOUT_QUERIES)))
        .skew(
                Double.parseDouble(prop.getProperty(WorkloadParameters.QUERY_SKEW)))
        .build();
        
        System.out.println(gp);
        System.out.println(wp);
        System.out.println(SystemParameters.instance());
        
        String[] nodes; //TODO also read nodenames from config file, and construct system params using that.
        //IAPIServer apiServer = APIServer.apiWithRemoteWorkNodes(nodes); 
        IAPIServer apiServer;
//        try
//        {
////            apiServer = APIServer.apiWithGivenWorkNodes(Arrays.asList(
////                                new CounterBackedWorkNode[]{new CounterBackedWorkNode()}));
//        
//        } catch (RemoteException e)
//        {
//            // TODO Auto-generated catch block    
//            throw new RuntimeException();
//        }
        
        apiServer = APIServer.apiWithRemoteWorkNodes(new String[]{"node0"});
        runBenchmark(gp, wp, apiServer);
        
        //TODO: get rid of vestigial 'maxDegree', figure what RMI names we need and how to parse them.
        System.exit(0);
    }
        
    public static void runBenchmark(GraphParameters graphParams, WorkloadParameters workloadParams, IAPIServer apiServer){
        
      Graph graph = SkewedDegreeGraph.makeSkewedDegreeGraph(graphParams); //TODO: change this
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
      System.out.println("done. log saved at: \n" + TimestampNameFileAppender.getLogName());
//
//      //reform this, we don't need it every time. but it may be useful to check basic behaviors
//      ProcessBuilder degreePlotting = new ProcessBuilder(
//              new String[]{
//                      "./plot.sh", 
//                      "--degree",  
//                      "foo", //placeholder, will delete soon
//                      String.valueOf(graphParams.getUpperDegreeBound()*1.2), //maxx
//                      String.valueOf(graphParams.getNumberVertices()/4), //maxy
//                      TimestampNameFileAppender.getLogName()})
//      .directory(new File("./logs"));
//      
//      ProcessBuilder latencyPlotting = new ProcessBuilder(
//              new String[]{
//                      "./plot.sh", 
//                      "--latency", 
//                      "foo", 
//                      "1000", 
//                      String.valueOf(7000), 
//                      TimestampNameFileAppender.getLogName()})
//      .directory(new File("./logs/"));
//      
//      for (String s: degreePlotting.command()){
//          System.out.printf("%s ", s);
//      }
//      
//      for (String s: latencyPlotting.command()) {
//          System.out.printf("%s ", s);
//      }
//      System.out.println("\n");
//      
// //     try
////      {
//////          Process p = degreePlotting.start();
//////          p.waitFor();    
//////          System.out.println("done with degree plot");
////          Process q = latencyPlotting.start();
////          q.waitFor();
////          
////          System.out.println("done with latency plot");
////          q.getOutputStream();
//      } catch (IOException e)
//      {
//          e.printStackTrace();
//      } catch (InterruptedException e)
//      {
//          e.printStackTrace();
//      }

    }   
}