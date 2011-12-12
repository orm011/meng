package com.twitter.dataservice.simulated;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.sharding.INodeSelectionStrategy;
import com.twitter.dataservice.sharding.ISharding;
import com.twitter.dataservice.sharding.RoundRobinShardLib;
import com.twitter.dataservice.sharding.PickFirstNodeShardLib;
import com.twitter.dataservice.sharding.TwoTierHashSharding;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;

import java.io.ObjectOutputStream.PutField;
import java.net.MalformedURLException;
import java.rmi.Naming;
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
     
    public static class FanoutTask implements Callable<Collection<Vertex>> {
        IDataNode rn;
        Vertex v;
        
        public FanoutTask(IDataNode rn, Vertex v){
            this.rn = rn;
            this.v = v;
        }
        
        @Override
        public Collection<Vertex> call() throws Exception
        {   
            return rn.getFanout(v);
        }
        
    };
    
    //TODO: test correctness of parallel version
    public Collection<Vertex> getFanout(Vertex v) {
        
      Collection<Node> destinations = shardinglib.getNodes(v);
      Collection<Vertex> ans = new LinkedList<Vertex>();

      List<Future<Collection<Vertex>>> futures = new LinkedList<Future<Collection<Vertex>>>();
        //TODO: make calls parallel
      for (Node n : destinations){
            futures.add(executor.submit(new FanoutTask(nodes.get(n), v)));
      } 
      
      for (Future<Collection<Vertex>> ft: futures){
          try {
              ans.addAll(ft.get());
          } catch (InterruptedException e)
          {
              throw new RuntimeException(e);
          } catch (ExecutionException e)
          {
              throw new RuntimeException(e);
          }
      }
        
      return ans;
    }

    public void putEdge(Edge e){
        Node n = shardinglib.getNode(e.getLeftEndpoint(), e.getRightEndpoint());
        
        try {
            nodes.get(n).putEdge(e);
        } catch (RemoteException re){
            throw new RuntimeException(re);
        }
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

    @Override
    public Collection<Vertex> getIntersection(Vertex v, Vertex w)
    {
        // TODO Auto-generated method stub
        return null;
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