package com.twitter.dataservice.simulated;
import com.google.common.primitives.Ints;
import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.sharding.INodeSelectionStrategy;
import com.twitter.dataservice.sharding.ISharding;
import com.twitter.dataservice.sharding.PickFirstNodeShardLib;
import com.twitter.dataservice.shardingpolicy.TwoTierHashSharding;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;
import java.net.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.NotImplementedException;

/*
 * Idea is to do less sequential work here
 * NOTE: not yet tested, may want to share some code with the other version.
 */

public class BriefAPIServer implements IAPIServer
{
    
    private static Logger log = LoggerFactory.getLogger(APIServer.class);
    
    public static int DEFAULT_PREALLOC_SIZE = 20; // median fanout size

    Map<Node, IDataNode> nodes = new HashMap<Node, IDataNode>();
    private INodeSelectionStrategy shardinglib = null; // see constructor 
    ExecutorService executor;
    
    //needed cause of existing test
    public static APIServer apiWithGivenWorkNodes(List<? extends IDataNode> givenNodes){
        Map<Node, IDataNode> nodes = new HashMap<Node, IDataNode>(givenNodes.size());
        
        for (int i = 0; i < givenNodes.size(); i++){
                nodes.put(new Node(i), givenNodes.get(i));
        }
        TwoTierHashSharding sh = TwoTierHashSharding.makeTwoTierHashFromNumExceptions(0, nodes, 5, 0, 0);
        return new APIServer(nodes, new PickFirstNodeShardLib(sh, null));
    }
    
    
    //can be used so everything runs in a single process. note sharding already depends on the nodes (needs to be consistent)
    public static APIServer makeServer(Map<Node, IDataNode> nodes, ISharding sharding){
        INodeSelectionStrategy shardinglib = new PickFirstNodeShardLib(sharding, null);        
        return new APIServer(nodes, shardinglib);
    }
    
    public static Map<Node, IDataNode> getRemoteNodes(String[] dataNodeNames, String[] dataNodeAddress, String[] dataNodePort){
        Map<Node, IDataNode> nodes = new HashMap<Node, IDataNode>(dataNodeNames.length);
        
        try {            
            for (int i = 0; i < dataNodeNames.length; i++){
                String name = dataNodeNames[i];
                String address = dataNodeAddress[i];
                int port = Integer.valueOf(dataNodePort[i]);
                
                log.debug("looking up registry at: {}:{}", address, port);
                Registry reg = LocateRegistry.getRegistry(address, port);
                log.debug("registry: {}",reg);
                log.debug("registry has list: {}", Arrays.toString(reg.list()));
                log.debug("looking up data node {}\n", name);
                IDataNode remote = (IDataNode) reg.lookup(name);
                log.debug("got data node: {}", remote);
                
                int id = Integer.parseInt(name.substring(name.split("[0-9]+", 0)[0].length(), name
                        .length()));
                
                Node local = new Node(id);
                nodes.put(local, remote);
            }            
        } catch (RemoteException e){
            throw new RuntimeException(e);
        } catch (NotBoundException e){
            throw new RuntimeException(e);
        }
        
        return nodes;
    }
            
    public BriefAPIServer(Map<Node, IDataNode> nodes, INodeSelectionStrategy shardinglib){
        this.nodes = nodes;
        this.shardinglib = shardinglib;
        executor = Executors.newFixedThreadPool(nodes.size());
    }

    public Edge getEdge(Vertex v, Vertex w){
        Node destination = shardinglib.getNode(v, w);
        //System.out.println("dest: " +destination);
        Edge result = null;

        try {
          result = nodes.get(destination).getEdge(v,w);
        } catch(RemoteException re){
          throw new RuntimeException(re);
        }

        return result;        
    }

    public static class IntersectionTask implements Callable<int[]>{
        IDataNode rn;
        Vertex v;
        Vertex w;
        int pageSize;
        int offset;
        
        public IntersectionTask(IDataNode rn, Vertex v, Vertex w, int pageSize, int offset){
            this.rn = rn;
            this.v = v;
            this.w = w;
            this.pageSize = pageSize;
            this.offset = offset;
        }
        
        @Override
        public int[] call() throws Exception
        {   
            return rn.getIntersection(v, w, pageSize, offset);
        }
    }
        
    public static class FanoutTask implements Callable<int[]> {
        IDataNode rn;
        Node n;
        Vertex v;
        int pageSize;
        int offset;
        
        
        public FanoutTask(IDataNode rn, Node n, Vertex v, int pageSize, int offset){
            this.rn = rn;
            this.n = n;
            this.v = v;
            this.pageSize = pageSize;
            this.offset = offset;
        }
        
        @Override
        public int[] call() throws Exception
        {   
        	//TODO: add a catcher for ConnectException
            return rn.getFanout(v, pageSize, offset);
        }
        
        @Override
        public String toString(){
            return String.format("FanoutTask: %s, %s, pageSize: %d, offset: %d", n, v, pageSize, offset);
        }
    };

    public Iterator<Integer> sortedScatterGather(List<Callable<int[]>> tasks, ExecutorService es){
    	
        List<Future<int[]>> futures = new LinkedList<Future<int[]>>();
        List<int[]> results = new LinkedList<int[]>();
        
        //scatter (may want to control better the executor service ie. a dedicated queue for each node)
        for (Callable<int[]> task : tasks){
              futures.add(es.submit(task));
        }
         
        //gather
        int i = 0;
        for (Future<int[]> ft: futures){
            int[] curr = new int[]{};
            try {
                curr = ft.get();
                log.debug("Success at task: {}", tasks.get(i));
            } catch (InterruptedException e)
            {
                log.warn("InterruptedException: {}. At task {} for node at position {}", new Object[]{e, tasks.get(i), i});
            } catch (ExecutionException e)
            {
                log.warn("ExecutionException: {}. At task {} for node at positon {}", new Object[]{e, tasks.get(i), i});                //throw new RuntimeException(e);
                if (e.getCause() instanceof ConnectException){
                	throw new RuntimeException(e);            		
                }
            }
            
            results.add(curr);
            i++;
        }
        
        ArrayList<Integer> answer = new ArrayList<Integer>(results.size());
        
        
        for (int[] res : results){
        	log.debug("{}", res.length);
        	if (res.length > 0){
        		answer.add(res[0]);
        	}
        }
        
        return answer.iterator();
    }
    /* 
     * currently we implement the pageSize and offset part of this by being pessimistic (since its hashed) and sticking to 
     * the discipline of bringing the followers in the right order
     */
    public List<Vertex> getFanout(Vertex v, int pageSize, int offset) {
      if (shardinglib instanceof TwoTierHashSharding) throw new UnsupportedOperationException("Don't use the old TwoTierHashSharding: deprecated");
      
      log.debug("using sharding lib: {}", shardinglib);
      Collection<Node> destinations = shardinglib.getNodes(v);
      log.debug("fanoutQ for vertex: {}, destinations are: {}", v, destinations);

      List<Callable<int[]>> fanoutTasks = new ArrayList<Callable<int[]>>(destinations.size());
      for (Node n: destinations){
          fanoutTasks.add(new FanoutTask(nodes.get(n), n, v, pageSize, offset));
      }

      Iterator<Integer> merged = sortedScatterGather(fanoutTasks, executor);
      
      List<Vertex> answer = new ArrayList<Vertex>(DEFAULT_PREALLOC_SIZE);
      while (merged.hasNext() && answer.size() < pageSize){
          answer.add(new Vertex(merged.next()));
      }
      return answer;
    }

    //@see putFanout()
    public void putEdge(Edge e){
        throw new NotImplementedException();
    }
    
    /*
     * NOTE: assumes the fanout array is ordered.
     */
    public void putFanout(int vertexid, int[] fanouts){
        
        Collection<Node> involved = shardinglib.getNodes(new Vertex(vertexid));
        
        //initialize lists to put stuff in
        Map<Node, ArrayList<Integer>> partitioned =  new HashMap<Node, ArrayList<Integer>>(involved.size());
        for (Node n: involved){
            partitioned.put(n, new ArrayList<Integer>(fanouts.length/nodes.size()));        
        }
        
        //pre-split fanout into all lists
        //NOTE: assumes stuff is already in order, so that the split is also in order.
        for (int destid : fanouts){
            Node n = shardinglib.getNode(new Vertex(vertexid), new Vertex(destid));
            partitioned.get(n).add(destid);
        }
        
        //deliver parts to each
        for (Node n : involved){
            try {
                nodes.get(n).putFanout(vertexid, Ints.toArray(partitioned.get(n)));
            } catch (RemoteException re){
                throw new RuntimeException(re);
            }
        }
    }
        
    @Override
    public List<Vertex> getIntersection(Vertex v, Vertex w, int pageSize, int offset){
        Collection<Node> nodesv = shardinglib.getNodes(v), nodesw = shardinglib.getNodes(w);

        List<Callable<int[]>> worktasks = new ArrayList<Callable<int[]>>(nodesv.size());
        Iterator<Integer> results;
        List<Vertex> answer;
            
        
            if (nodesv.equals(nodesw)){
                //TODO: verify this accounts for the order of elements. change getNodes() method
                //to not abstract away the element order like it does right now.
                //TODO: maybe change sharding function so that if we hash to the same set of machines,
                //then we map edges to the same exact machine list, rather than a permutation.
                //System.out.println("same nodes");
                //in this case we can get the already intersected stuff from each of them independently
                for (Node shardnode : nodesv){
                    worktasks.add(new IntersectionTask(nodes.get(shardnode), v, w, pageSize, offset));
                }
                
                results = sortedScatterGather(worktasks, executor);
                
                answer = new ArrayList<Vertex>(pageSize);
                while (results.hasNext() & answer.size() < pageSize){
                    answer.add(new Vertex(results.next()));
                }
            } else {
                //bring in the full fanout, intersect here.
                //System.out.println("different nodes");
                /*
                 *TODO 
                 *2) figure if there are easy ways to avoid
                bringing all of it at once ie. optimal amount of fanout to bring in before we run the intersections. 
                3) evaluate pushing operation to one of the nodes. (not as needed here because we are only measuring latency,
                in this case the API server would be a bottleneck if the Client weren't it.
                TODO: more careful analysis of the latency effects of intersecting here as well as whether I need to measure
                 throughput in order to exhibit the benefits of the partitioning strategies (as opposed to only latency)
                */
                Collection<Vertex> fanoutv = getFanout(v, Integer.MAX_VALUE, offset);
                Collection<Vertex> fanoutw = getFanout(w, Integer.MAX_VALUE, offset);                
                int[] intersect = UtilMethods.intersectSortedUniqueArraySet(Vertex.toIntArray(fanoutv), Vertex.toIntArray(fanoutw), pageSize, offset);
                //System.out.println(Arrays.toString(intersect));
                answer = UtilMethods.toVertexCollection(intersect);
            }

        return answer;
                
    }
    
    //TODO: unit test
    public int totalLoad(){
        int total = 0;
        for (IDataNode n: nodes.values()){
            try
            {
                total += n.totalLoad();
            } catch (RemoteException e)
            {
                throw new RuntimeException(e);
            }
        }        
        return total;
    }

	@Override
	public Collection<IAPIServer.Stats> stat() {
		List<IAPIServer.Stats> ans = new ArrayList<IAPIServer.Stats>(nodes.values().size());
		
		for (IDataNode n: nodes.values()){
			try {
				long start = System.nanoTime();
				IDataNode.NodeStats ns = n.stat();
				long duration = System.nanoTime() - start;
				
				ans.add(new IAPIServer.SuccessfulStats(duration, ns));
			} catch (RemoteException re){
				ans.add(new IAPIServer.FailedStats(re));
			}
		}
		
		return ans;
	}
	
	public static SuccessfulStats statSummary(Collection<Stats> stats){
		Iterator<Stats> st = stats.iterator();
		SuccessfulStats total = SuccessfulStats.EMPTY;
		
		while (st.hasNext()){
			Stats x = st.next();
			if (x instanceof SuccessfulStats){
				total = total.merge((SuccessfulStats)x);
			}
		}
		
		return total;
	}
}