package com.twitter.dataservice.simulated;

import com.twitter.dataservice.remotes.ICompleteWorkNode;
import com.twitter.dataservice.remotes.IUncheckedWorkDataNode;
import com.twitter.dataservice.sharding.IShardLib;
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

public class APIServer
{
    //TODO: make it behave like an individual node? implement the same interface?
    Map<Node, ICompleteWorkNode> nodes = new HashMap<Node, ICompleteWorkNode>();
    private IShardLib shardinglib = null; // see constructor

    
    public APIServer(String[] dataNodeNames) {
        try {
            for (String name: dataNodeNames){
                System.out.printf("looking up node %s\n", name);
              
                ICompleteWorkNode remote = (ICompleteWorkNode) Naming.lookup(name);
                int id = Integer.parseInt(name.substring(name.split("[0-9]+", 0)[0].length(), name
                    .length()));
                Node local = new Node(id);
                nodes.put(local, remote);
            }

            shardinglib = new PickFirstNodeShardLib(new TwoTierHashSharding(new ArrayList<Vertex>(), new ArrayList<Node>(nodes.keySet()), 5, 0, 0), null);
            //shardinglib  = new RoundRobinShardLib(nodes.size());
        } catch (RemoteException e) {
            System.out.println("failed to find remote node: " + e.getMessage());
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e){
            e.printStackTrace();
        }
    }


    public Edge getEdge(Vertex v, Vertex w){
        System.out.println("hello");
        Node destination = shardinglib.getNode(new Edge(v, w));
        Edge result = null;

        try {
          result = nodes.get(destination).getEdge(new Vertex(1), new Vertex(2));
        } catch(RemoteException re){
          throw new RuntimeException(re);
        }

        return result;        
    }

    public Collection<Vertex> getAllEdges(Vertex v) {
      Collection<Node> destinations = shardinglib.getNodes(v);
      Collection<Vertex> ans = null;

      try{
        //TODO: make calls parallel
        for (Node n : destinations){
            System.out.println(n);
            ans = nodes.get(n).getFanOut(v);
        }
        
      } catch (RemoteException re){
          throw new RuntimeException(re);
      }

      return ans;
    }
    
    public static void main(String[] args){
        APIServer api = new APIServer(args);
        System.out.println("about the request...");
        Edge mark1 = api.getEdge(new Vertex(1), new Vertex(2));
        System.out.println(mark1);
        
        Collection<Vertex> vertices = api.getAllEdges(new Vertex(1));
        System.out.println(vertices);
        
        
        //new Benchmark(api).run();
    }
}