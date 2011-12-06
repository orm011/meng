package com.twitter.dataservice.simulated;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.sharding.INodeSelectionStrategy;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class APIServer implements IAPIServer
{
    //TODO: make it api have interface like an individual node?;
    Map<Node, IDataNode> nodes = new HashMap<Node, IDataNode>();
    private INodeSelectionStrategy shardinglib = null; // see constructor

    //everything runs in one process
    public static APIServer apiWithGivenWorkNodes(List<? extends IDataNode> givenNodes){
        Map<Node, IDataNode> nodes = new HashMap<Node, IDataNode>(givenNodes.size());
        
        for (int i = 0; i < givenNodes.size(); i++){
                nodes.put(new Node(i), givenNodes.get(i));
        }
        
        return new APIServer(nodes);
    }
    
    //using RMI for multiple processes, assumes the other processes have been started
    //and have registered
    public static APIServer apiWithRemoteWorkNodes(String[] dataNodeNames, String[] dataNodeAddress, String[] dataNodePort){
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
        
        return new APIServer(nodes);
    }

    private APIServer(Map<Node, IDataNode> nodes){
        this.nodes = nodes;
        shardinglib = new PickFirstNodeShardLib(new TwoTierHashSharding(new ArrayList<Vertex>(), new ArrayList<Node>(nodes.keySet()), 5, 0, 0), null);        
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
    ExecutorService executor = Executors.newFixedThreadPool(1);
    
    public Collection<Vertex> getFanout(Vertex v) {
      Collection<Node> destinations = shardinglib.getNodes(v);
      Collection<Vertex> ans = null;

      try{
        //TODO: make calls parallel
        for (Node n : destinations){
            ans = nodes.get(n).getFanout(v);
        }
        
      } catch (RemoteException re){
          throw new RuntimeException(re);
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
    
    public static void main(String[] args){ 
        if (args.length != 3) {
            System.out.println("usage: name address port");
            System.exit(1);
        }
        
        String name = args[0];
        String address = args[1];
        String port = args[2];
        
        System.out.println("about to bind...");
        APIServer api = APIServer.apiWithRemoteWorkNodes(new String[]{name}, new String[]{address}, new String[]{port});
        System.out.println("total load: " + api.totalLoad());
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