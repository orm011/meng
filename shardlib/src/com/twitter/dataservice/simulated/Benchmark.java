package com.twitter.dataservice.simulated;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Ticker;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Ints;
import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.sharding.INodeSelectionStrategy;
import com.twitter.dataservice.shardingpolicy.LookupTableSharding;
import com.twitter.dataservice.shardingpolicy.SimpleTwoTierSharding;
import com.twitter.dataservice.shardingpolicy.TwoTierHashSharding;
import com.twitter.dataservice.shardingpolicy.VertexHashSharding;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Pair;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.parameters.GraphParameters;
import com.twitter.dataservice.parameters.SamplableBuilder;
import com.twitter.dataservice.parameters.SystemParameters;
import com.twitter.dataservice.parameters.WorkloadParameters;
import com.twitter.dataservice.parameters.SamplableBuilder.DistributionType;

public class Benchmark {

    static org.slf4j.Logger logger;
        
    public static void main(String[] args) {
        //TODO: read some logname marker string from the config as well. will help in selecting all the logs 
        // for analysis. 
        if (args.length != 2){
            System.out.println("usage: logPropertyFile benchmarkPropertyFile");
            System.exit(1);
        }
        
        String logPropertyFile = args[0];
        String benchmarkPropertyFile = args[1];
        
        PropertyConfigurator.configure(logPropertyFile);        
        logger = org.slf4j.LoggerFactory.getLogger(Benchmark.class);        
        
        Properties prop = parsePropertyFile(benchmarkPropertyFile); 
        logger.info(prop.toString());
        GraphParameters gp = getGraphParams(prop); 
        logger.info(gp.toString());

        WorkloadParameters wp = getWorkloadParams(prop);
        logger.info(wp.toString());
        
        int numNodes = Integer.parseInt(prop.getProperty(SystemParameters.NUM_DATA_NODES));

        SkewedDegreeGraph graph = SkewedDegreeGraph.makeSkewedDegreeGraph(gp);
        Map<Node, IDataNode> nodes = setupNodes(prop, numNodes);    
        INodeSelectionStrategy sh = setupShardingPolicy(prop, numNodes, graph);
        logger.info(sh.getClass().getCanonicalName());

        IAPIServer apiServer = new APIServer(nodes, sh);
        runBenchmark(graph, gp, wp, apiServer);
        System.exit(0);
    }
    
    
    private static INodeSelectionStrategy setupShardingPolicy(Properties prop, int numNodes, SkewedDegreeGraph graph) {
        String policy = prop.getProperty("system.shardingPolicy");         
        int ROUGHNUMLOOKUP = 9000000; //abt 9 million in lookup table
        
        //currently, if using the other strategies, is tries to read a file for the exceptions.
        //need to change this so it uses benchmark itself?
        INodeSelectionStrategy sh = null;
        System.out.println(policy);
        if (policy.equals("sharding.vertex")){
            Integer numShards = Integer.parseInt(prop.getProperty("vertex.numShards"));
            sh = new VertexHashSharding(numNodes, numShards);
        } else if (policy.equals("sharding.lookup")){
            String exceptionsFile = prop.getProperty("lookup.exceptions");
            //change to make the 2 tier transparent?

            //NOTE: this way of construction may be wrong if the exceptions passed to special are somehow
            //different to the ones passed to lookuptable
            INodeSelectionStrategy special = new LookupTableSharding(exceptionsFile, ROUGHNUMLOOKUP, "\t");
            int[] specialids = loadExceptionFile(exceptionsFile);
            System.out.println("exceptions: " + Arrays.toString(Arrays.copyOf(specialids, 5)));
            System.out.println("exceptions size: " + specialids.length);
            sh = new SimpleTwoTierSharding(new VertexHashSharding(numNodes, 1), special, specialids);
        } else if (policy.equals("sharding.twoTier")){
            Integer numShards = Integer.parseInt(prop.getProperty("twoTier.numShards"));
            logger.info("numShards: " + numShards);
            String exceptions = prop.getProperty("twoTier.exceptions");
            logger.info("exceptions form file: " + exceptions);
            int[] specialids = loadExceptionFile(exceptions);
            sh = new SimpleTwoTierSharding(new VertexHashSharding(numNodes), 
                    new VertexHashSharding(numNodes, numShards), specialids);
        } else if (policy.equals("sharding.syntheticTwoTier")) {
            Integer threshold = Integer.parseInt(prop.getProperty("syntheticTwoTier.threshold"));
            Integer numShards = Integer.parseInt(prop.getProperty("syntheticTwoTier.numShards"));
            logger.info("threshold {}", threshold);
            logger.info("numShards {}", numShards);

            ArrayList<Integer> specialidsBuilder = new ArrayList<Integer>();
            Iterator<Pair<Integer, int[]>> it = graph.fanoutIterator();
            
            while (it.hasNext()){
            	Pair<Integer, int[]> curr = it.next();
            	if (curr.getRight().length > threshold){
            		specialidsBuilder.add(curr.getLeft());
            	}
            }

            int[] specialids = Ints.toArray(specialidsBuilder);
            logger.info("exception set {}..., size {}", 
            		Arrays.toString(Arrays.copyOf(specialids, Ints.min(5, specialids.length))), 
            		specialids.length);

            sh = new SimpleTwoTierSharding(new VertexHashSharding(numNodes, 1), 
                    new VertexHashSharding(numNodes, numShards), specialids);
        } else {
            throw new RuntimeException(String.format("invalid system.shardingPolicy: %s", policy));
        }
      
        return sh;
	}


	private static Map<Node, IDataNode> setupNodes(Properties prop, int numNodes) {
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

        return APIServer.getRemoteNodes(names, address, ports);
	}


	static Properties parsePropertyFile(String benchmarkPropertyFile) {
        Properties prop = new Properties();
        try
        {
            prop.load(new FileInputStream(benchmarkPropertyFile));
        } catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        
        return prop;
	}
	
	public final static String distributionType = "distributionType";
	public final static String distribution = "distribution";
	
	protected static Samplable getSamplable(Properties prop, String namePrefix){
		String typeproperty = namePrefix + "." + distributionType;
        String distType = prop.getProperty(typeproperty);
        DistributionType dt = DistributionType.valueOf(distType);
        SamplableBuilder sb = new SamplableBuilder();
        sb.setType(dt);
        
        String newprefix = namePrefix + "." + distribution + ".";
        
        if (dt.equals(DistributionType.ZIPF)){
        	float exp = Float.valueOf(prop.getProperty(newprefix + ZipfSamplable.Parameters.exponent.toString()));
        	int lower = Integer.valueOf(prop.getProperty(newprefix + ZipfSamplable.Parameters.lower.toString()));
        	int upper = Integer.valueOf(prop.getProperty(newprefix + ZipfSamplable.Parameters.upper.toString()));
        	int resolution = Integer.valueOf(prop.getProperty(newprefix + ZipfSamplable.Parameters.resolution.toString()));
        	sb.setSkew(exp); sb.setLowLimit(lower); sb.setHighLimit(upper); sb.setZipfbins(resolution);
        } else if (dt.equals(DistributionType.CONSTANT)){
        	int value = Integer.valueOf(prop.getProperty(newprefix + ConstantSamplable.Parameters.value.toString()));
        	sb.setValue(value);
        } else if (dt.equals(DistributionType.UNIFORM)){
        	int lower = Integer.valueOf(prop.getProperty(newprefix + UniformSamplable.Parameters.lower.toString()));
        	int upper = Integer.valueOf(prop.getProperty(newprefix + UniformSamplable.Parameters.upper.toString()));
        	sb.setLowLimit(lower); sb.setHighLimit(upper);
        } else {
        	throw new NotImplementedException(dt.toString());
        }

        return sb.build();
	}

	protected static WorkloadParameters getWorkloadParams(Properties prop) {
        int num = Integer.parseInt(prop.getProperty(WorkloadParameters.NUMBER_OF_QUERIES));
        int edge = Integer.parseInt(prop.getProperty(WorkloadParameters.PERCENT_EDGE_QUERIES));
        int fanout = Integer.parseInt(prop.getProperty(WorkloadParameters.PERCENT_FANOUT_QUERIES));
        int intersection = Integer.parseInt(prop.getProperty(WorkloadParameters.PERCENT_INTERSECTION_QUERIES));

        Samplable s = getSamplable(prop, WorkloadParameters.prefix);
        WorkloadParameters wp = new WorkloadParameters(num, edge, fanout, intersection, s);        
        return wp;
	}


	protected static GraphParameters getGraphParams(Properties prop) {
        int numVer = Integer.parseInt(prop.getProperty(GraphParameters.NUMBER_VERTICES));

        Samplable samp = getSamplable(prop, GraphParameters.prefix);
        GraphParameters gp = new GraphParameters(numVer, samp);
                
        return gp;
	}
	    
    public static int[] loadExceptionFile(String filename){
        //format is %id\t...whatever else 
        
        ArrayList<Integer> accumulator = new ArrayList<Integer>();
        
        BufferedReader br;
        try
        {
            br = new BufferedReader(new FileReader(filename));
        } catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
        
        String current;
        try
        {
            while ((current = br.readLine()) != null){
                String[] parts = current.split("[\t| ]", 2);
                accumulator.add(Integer.parseInt(parts[0]));
            }
        } catch (NumberFormatException e)
        {
            throw new RuntimeException(e);
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        
        return Ints.toArray(accumulator);        
    }
    
    public static final String NUM_ORDINARY_SHARDS = "sharding.numOrdinaryShards";
    public static final String NUM_SHARDS_PER_EXCEPTION = "sharding.numShardsPerException";
    public static final String NUM_NODES_PER_EXCEPTION = "sharding.numNodesPerException";
    public static final String NUM_EXCEPTIONS = "sharding.numExceptions";
    
    public static void runBenchmark(SkewedDegreeGraph graph, GraphParameters graphParams, WorkloadParameters workloadParams, IAPIServer apiServer){
      MetricsCollector omc = new LatencyTrackingAPIServer.InMemoryCollector(graphParams, workloadParams);
      IAPIServer api = new LatencyTrackingAPIServer(apiServer, omc);      
      
      Iterator<Pair<Integer, int[]>> it = graph.fanoutIterator();
      omc.begin();
      
      Collection<IAPIServer.Stats> nodestats = api.stat();
      logger.info(prettyPrintStats(nodestats));

      int i = 0;      
      Ticker t = Ticker.systemTicker();
      long start = t.read();
      while (it.hasNext()){
          Pair<Integer, int[]> current = it.next();
          logger.debug("puting vertex {}, array length: {}", current.getLeft(), current.getRight().length);
          api.putFanout(current.getLeft(), current.getRight());
          i++;
      }

      System.out.printf("done loading graph. load time = %d musec\n", (t.read() - start)/1000);
      logger.info(prettyPrintStats(api.stat()));
      Iterator<Query> ot = graph.workloadIterator(workloadParams);
      
      while (ot.hasNext()) {
          Query q = ot.next();
          List<Vertex> answer;
          
          //TODO: be more specific about the exception, at the work node level maybe?
          try {
              logger.debug("{}", q);
              answer = q.execute(api);
              logger.debug("{}", answer.subList(0, Ints.min(10, answer.size())));
          } catch (RuntimeException re){
        	  re.printStackTrace();
              logger.error("{} for query: {}", re.getMessage(), q);
          }          
      }
 
      omc.finish();
      logger.info(prettyPrintStats(api.stat()));
      System.out.println("done running benchmark. log saved at: \n" + TimestampNameFileAppender.getLogName());
    }


	private static String prettyPrintStats(Collection<IAPIServer.Stats> nodestats) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("Node Stats:\n");
		for (IAPIServer.Stats st : nodestats){
			  sb.append("\t");
			  sb.append(st.toString());
			  sb.append("\n");
		}
		
		return sb.toString();
	}
}