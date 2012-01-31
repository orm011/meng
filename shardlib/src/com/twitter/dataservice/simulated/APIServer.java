package com.twitter.dataservice.simulated;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import com.twitter.dataservice.parameters.SystemParameters;
import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.sharding.INodeSelectionStrategy;
import com.twitter.dataservice.sharding.ISharding;
import com.twitter.dataservice.sharding.PickFirstNodeShardLib;
import com.twitter.dataservice.shardingpolicy.RoundRobinShardLib;
import com.twitter.dataservice.shardingpolicy.TwoTierHashSharding;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;

import java.nio.channels.ScatteringByteChannel;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/*
 * # Set Cursor = -1 when requesting the first Page. Cursor = 0 indicates the end of the result set.
struct Page {
  1: i32 count
  2: i64 cursor
}
 */

public class APIServer implements IAPIServer
{
    public static int DEFAULT_PREALLOC_SIZE = 20; // median fanout size
    //TODO: make it api have interface like an individual node?;
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
                
                System.out.printf("looking up node: %s\n", name);
                Registry reg = LocateRegistry.getRegistry(address, port);
                IDataNode remote = (IDataNode) reg.lookup(name);
                System.out.println("got remote reference: " + remote);
                //Naming.lookup(name);
                
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
            
    public APIServer(Map<Node, IDataNode> nodes, INodeSelectionStrategy shardinglib){
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
        Vertex v;
        int pageSize;
        int offset;
        
        public FanoutTask(IDataNode rn, Vertex v, int pageSize, int offset){
            this.rn = rn;
            this.v = v;
            this.pageSize = pageSize;
            this.offset = offset;
        }
        
        @Override
        public int[] call() throws Exception
        {   
            return rn.getFanout(v, pageSize, offset);
        }
        
    };

    public Iterator<Integer> sortedScatterGather(List<Callable<int[]>> tasks, ExecutorService es){

        List<Future<int[]>> futures = new LinkedList<Future<int[]>>();
        List<Iterator<Integer>> results = new LinkedList<Iterator<Integer>>();
        
        //scatter (may want to control better the executor service ie. a dedicated queue for each node)
        for (Callable<int[]> task : tasks){
              futures.add(es.submit(task));
        }
         
        //gather
        for (Future<int[]> ft: futures){
            try {
                results.add(Ints.asList(ft.get()).iterator());
            } catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            } catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        }

        //merge in sorted order
        Iterator<Integer> merged = Iterators.mergeSorted(results, Collections.reverseOrder(Collections.reverseOrder()));
        return merged;
    }
    /* 
     * currently we implement the pageSize and offset part of this by being pessimistic (since its hashed) and sticking to 
     * the discipline of bringing the followers in the right order
     */
    public List<Vertex> getFanout(Vertex v, int pageSize, int offset) {
      if (shardinglib instanceof TwoTierHashSharding) throw new UnsupportedOperationException("Don't use the old TwoTierHashSharding: deprecated");
       
      Collection<Node> destinations = shardinglib.getNodes(v);

      List<Callable<int[]>> fanoutTasks = new ArrayList<Callable<int[]>>(destinations.size());
      for (Node n: destinations){
          fanoutTasks.add(new FanoutTask(nodes.get(n), v, pageSize, offset));
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
    
    //TODO: we need to compute destination edges on a 'per edge' basis.
    //we can still do that and batch send after.
    //can be parallelized
    //THIS IS INTENTIONALY BROKEN EXCEPT FOR VERY SPECIAL CASES
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
        
        //split fanout into the four lists
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
}