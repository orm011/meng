package com.twitter.dataservice.simulated;

import com.twitter.dataservice.remotes.ICompleteWorkNode;
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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class APIServer implements IAPIServer
{
    //TODO: make it api have interface like an individual node?;
    Map<Node, ICompleteWorkNode> nodes = new HashMap<Node, ICompleteWorkNode>();
    private INodeSelectionStrategy shardinglib = null; // see constructor

    //everything runs in one process
    public static APIServer apiWithGivenWorkNodes(List<? extends ICompleteWorkNode> givenNodes){
        Map<Node, ICompleteWorkNode> nodes = new HashMap<Node, ICompleteWorkNode>(givenNodes.size());
        
        for (int i = 0; i < givenNodes.size(); i++){
                nodes.put(new Node(i), givenNodes.get(i));
        }
        
        return new APIServer(nodes);
    }
    
    //using RMI for multiple processes, assumes the other processes have been started
    //and have registered
    public static APIServer apiWithRemoteWorkNodes(String[] dataNodeNames){
        Map<Node, ICompleteWorkNode> nodes = new HashMap<Node, ICompleteWorkNode>(dataNodeNames.length);
        
        try {            
            for (String name: dataNodeNames){
                System.out.printf("looking up node %s\n", name);
              
                ICompleteWorkNode remote = (ICompleteWorkNode) Naming.lookup(name);
                int id = Integer.parseInt(name.substring(name.split("[0-9]+", 0)[0].length(), name
                    .length()));
                Node local = new Node(id);
                nodes.put(local, remote);
            }            
        } catch (RemoteException e){
            throw new RuntimeException(e);
        } catch (NotBoundException e){
            throw new RuntimeException(e);
        } catch (MalformedURLException e){
            throw new RuntimeException(e);
        } 
        
        return new APIServer(nodes);
    }

    private APIServer(Map<Node, ICompleteWorkNode> nodes){
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
    
    //TODO: move this. Right now this tests whether the RMI works and prints to see 
    //if results make sense.
    public static void main(String[] args){
        APIServer api = APIServer.apiWithRemoteWorkNodes(args);
        System.out.println("about to request...");
        Edge mark1 = api.getEdge(new Vertex(1), new Vertex(2));
        System.out.println(mark1);
        
        Collection<Vertex> vertices = api.getFanout(new Vertex(1));
        System.out.println(vertices);
    }

    @Override
    public Collection<Vertex> getIntersection(Vertex v, Vertex w)
    {
        // TODO Auto-generated method stub
        return null;
    }
}