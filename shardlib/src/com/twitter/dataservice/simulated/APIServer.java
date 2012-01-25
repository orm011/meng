package com.twitter.dataservice.simulated;

import com.google.common.primitives.Ints;
import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.sharding.INodeSelectionStrategy;
import com.twitter.dataservice.sharding.ISharding;
import com.twitter.dataservice.sharding.RoundRobinShardLib;
import com.twitter.dataservice.sharding.PickFirstNodeShardLib;
import com.twitter.dataservice.shardingpolicy.TwoTierHashSharding;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;


public class APIServer implements IAPIServer
{
    //TODO: make it api have interface like an individual node?;
    Map<Node, IDataNode> nodes = new HashMap<Node, IDataNode>();
    private INodeSelectionStrategy shardinglib = null; // see constructor

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
                
                //TODO: move this elsewhere? don't want to always reset.
                remote.reset();
            }            
        } catch (RemoteException e){
            throw new RuntimeException(e);
        } catch (NotBoundException e){
            throw new RuntimeException(e);
        }
        
        return nodes;
    }
        
    //TODO: make api server take shardling lib (construct that first?)
    //TODO: make shardling lib take parameters: #shards, etc.
    //TODO: figure out how to not repeat work 
    
    private APIServer(Map<Node, IDataNode> nodes, INodeSelectionStrategy shardinglib){
        this.nodes = nodes;
        this.shardinglib = shardinglib;       
    }

    public Edge getEdge(Vertex v, Vertex w){
        Node destination = shardinglib.getNode(v, w);
        Edge result = null;

        try {
          result = nodes.get(destination).getEdge(new Vertex(1), new Vertex(2));
        } catch(RemoteException re){
          throw new RuntimeException(re);
        }

        return result;        
    }
    
    //TODO later: put the #nodes in system in the constructor arg.
    ExecutorService executor = Executors.newFixedThreadPool(2);
     
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

    
    public static Collection<Vertex> toVertexCollection(int[] ids){
        Collection<Vertex> wrap = new ArrayList<Vertex>(ids.length);
        
        for (int id: ids){
            wrap.add(new Vertex(id));
        }
        
        return wrap;
    }
    
    
    //TODO: this won't work for the version with 2-tier stuff
    public Collection<Vertex> getFanout(Vertex v, int pageSize, int offset) {
      if (shardinglib instanceof TwoTierHashSharding) throw new UnsupportedOperationException();
       
      Collection<Node> destinations = shardinglib.getNodes(v);
      int[] ans = new int[]{};

      List<Future<int[]>> futures = new LinkedList<Future<int[]>>();
        //TODO: make calls parallel
      for (Node n : destinations){
            futures.add(executor.submit(new FanoutTask(nodes.get(n), v, pageSize, offset)));
      } 
      
      for (Future<int[]> ft: futures){
          try {
              Ints.concat(ans, ft.get());
          } catch (InterruptedException e)
          {
              throw new RuntimeException(e);
          } catch (ExecutionException e)
          {
              throw new RuntimeException(e);
          }
      }
        
      return toVertexCollection(ans);
    }

    public void putEdge(Edge e){
        //currently loading each remote node separately
        //TODO: make this configurable, may want to use this to help me test.
        throw new NotImplementedException();
//        Node n = shardinglib.getNode(e.getLeftEndpoint(), e.getRightEndpoint());
//        
//        try {
//            nodes.get(n).putEdge(e);
//        } catch (RemoteException re){
//            throw new RuntimeException(re);
//        }
    }   
    
    @Deprecated
    public static void main(String[] args){ 
        if (args.length != 3) {
            System.out.println("usage: name address port");
            System.exit(1);
        }
        
        String name = args[0];
        String address = args[1];
        String port = args[2];
        //TODO: correct
        System.out.println("about to bind...");
        System.exit(1);
        //TODO: change to use new interface
        //APIServer api = APIServer.apiWithRemoteWorkNodes(new String[]{name}, new String[]{address}, new String[]{port}, 1);
        //System.out.println("total load: " + api.totalLoad());
    }
    
   /*
    * right now this implementation only works when id-ranges in fanouts for a particular node  are
    * all located in matching positions in the list.
    */
    @Override
    public Collection<Vertex> getIntersection(Vertex v, Vertex w, int pageSize, int offset){
        Collection<Node> nodesv = shardinglib.getNodes(v), nodesw = shardinglib.getNodes(w);
        assert (nodesv.size() == 1 && nodesw.size() == 1);
        
        Node vnode = nodesv.iterator().next(), wnode = nodesw.iterator().next();
        
        int[] result;
        
        //TODO: offset + pagesize in the remote case. want to use limited fanouts
        try {
            if (vnode.equals(wnode)){
                result = nodes.get(vnode).getIntersection(v, w, pageSize, offset);
            } else {
                //TODO 1) parallelize fanout calls 2) figure if there are good ways to avoid
                //bringing all of it at once ie. optimal amount of fanout to bring in before we run the intersections. 
                //3) evaluate pushing operation to one of the nodes. (not as needed here because we are only measuring latency,
                //in this case the API server would be a bottleneck if the Client weren't it.
                //TODO: more careful analysis of the latency effects of intersecting here as well as whether I need to measure
                // throughput in order to exhibit the benefits of the partitioning strategies (as opposed to only latency)
                
                int[] vfanout = nodes.get(vnode).getFanout(v, Integer.MAX_VALUE, -1);
                int[] wfanout = nodes.get(wnode).getFanout(w, Integer.MAX_VALUE, -1);
                
                result = UtilMethods.intersectSortedUniqueArraySet(vfanout, wfanout, pageSize, offset);
            }
        } catch (RemoteException re){
            throw new RuntimeException();
        }
        
        
        Collection<Vertex> wrap = new ArrayList<Vertex>();
        
        for (int id:result){
            wrap.add(new Vertex(id));
        }
        
        return wrap;
    }
    
    //TODO: unit test?
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