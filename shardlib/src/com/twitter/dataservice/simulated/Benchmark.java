package com.twitter.dataservice.simulated;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.sharding.TwoTierHashSharding;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.parameters.GraphParameters;
import com.twitter.dataservice.parameters.SystemParameters;
import com.twitter.dataservice.parameters.WorkloadParameters;

public class Benchmark {

    static org.slf4j.Logger logger;
        
    public static void main(String[] args) {
        //TODO: read some logname marker string from the config as well. will help in selecting all the logs 
        // for analysis. 
        //TODO: make plot optional 
        if (args.length != 2){
            System.out.println("usage: logPropertyFile benchmarkPropertyFile");
            System.exit(1);
        }
        
        String logPropertyFile = args[0];
        String benchmarkPropertyFile = args[1];
        
        PropertyConfigurator.configure(logPropertyFile);
        
        logger = org.slf4j.LoggerFactory.getLogger(Benchmark.class);        
        
        Properties prop = new Properties();
        
        try
        {
            prop.load(new FileInputStream(benchmarkPropertyFile));
            
            //copy the properties file as a whole int stdio and the log.
            BufferedReader properties = new BufferedReader(new FileReader(new File(benchmarkPropertyFile)));
            String current;
            while ((current = properties.readLine()) != null){
                System.out.println(current);
                logger.debug(current);
            }
            
            System.out.println();
            
        } catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        
        //set up graph
        GraphParameters gp = new GraphParameters.Builder()
        .degreeBoundAndTargetAvg(
                Integer.parseInt(prop.getProperty(GraphParameters.DEGREE_RATIO_BOUND)), 
                Integer.parseInt(prop.getProperty(GraphParameters.AVERAGE_DEGREE)))
        .degreeSkew(
                Double.parseDouble(prop.getProperty(GraphParameters.SKEW_PARAMETER)))
        .numberVertices(
                Integer.parseInt(prop.getProperty(GraphParameters.NUMBER_VERTICES))).
        build();
        
        //set up workload
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

        //reset system params
        int numNodes = Integer.parseInt(prop.getProperty(SystemParameters.NUM_DATA_NODES));
        int edgeWeight = Integer.parseInt(prop.getProperty(SystemParameters.PER_EDGE_WEIGHT));
        SystemParameters.reset(edgeWeight, numNodes);
        
        //print all parsed params
        System.out.println(gp);
        System.out.println(wp);
        System.out.println(SystemParameters.instance());
        System.out.println();
        
        //init shardlib, apiServer
        String[] names = new String[numNodes];
        String[] address = new String[numNodes];
        String[] ports = new String[numNodes];        
        for (int i = 0; i < numNodes; i++){
            names[i] = "node" + i;
            String nodeAddress = prop.getProperty(names[i]);
            if (nodeAddress == null) throw new IllegalArgumentException();

            String[] addressWithPort = nodeAddress.split(":");
            address[i] = addressWithPort[0];
            ports[i] = addressWithPort[1];
        }
        
        int numOrdinaryShards = Integer.parseInt(prop.getProperty(NUM_ORDINARY_SHARDS));
        int numShardsPerException; // = Integer.parseInt(prop.getProperty(NUM_SHARDS_PER_EXCEPTION));
        int numNodesPerException; // = Integer.parseInt(prop.getProperty(NUM_EXCEPTIONS));
        int numExceptions; // = Integer.parseInt(prop.getProperty(NUM_EXCEPTIONS));
 
        //NOTE: remove this for longer term experiments. for now this is reasonable
        numShardsPerException = SystemParameters.instance().numDataNodes;
        numNodesPerException = SystemParameters.instance().numDataNodes;
        numExceptions = gp.getNumberVertices();
        
        //TODO: sanity check sharding is splitting things reasonably evenly and log relative sizes.
        Map<Node, IDataNode> nodes = APIServer.getRemoteNodes(names, address, ports);
        TwoTierHashSharding sh = TwoTierHashSharding.makeTwoTierHashFromNumExceptions(
                numExceptions, 
                nodes, 
                numOrdinaryShards, 
                numShardsPerException, 
                numNodesPerException);
        
        IAPIServer apiServer = APIServer.makeServer(nodes, sh);        
        
        runBenchmark(gp, wp, apiServer);
        System.exit(0);
    }
    
    public static final String NUM_ORDINARY_SHARDS = "sharding.numOrdinaryShards";
    public static final String NUM_SHARDS_PER_EXCEPTION = "sharding.numShardsPerException";
    public static final String NUM_NODES_PER_EXCEPTION = "sharding.numNodesPerException";
    public static final String NUM_EXCEPTIONS = "sharding.numExceptions";
    
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
    }   
}