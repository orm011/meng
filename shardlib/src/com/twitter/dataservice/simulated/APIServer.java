package com.twitter.dataservice.simulated;

import com.twitter.dataservice.remotes.RemoteDataNode;
import com.twitter.dataservice.sharding.ISharding;
import com.twitter.dataservice.shardlib.DirectHash;
import com.twitter.dataservice.shardlib.Edge;
import com.twitter.dataservice.shardlib.Node;
import com.twitter.dataservice.shardlib.Vertex;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class APIServer
{
    private ISharding shardinglib = null;
    
    List<RemoteDataNode> datanodes = new LinkedList<RemoteDataNode>();
    
    public APIServer(String[] dataNodeNames) {
        
        try {
            for (String name: dataNodeNames){
                System.out.printf("looking up node %s\n", name);
                datanodes.add((RemoteDataNode) Naming.lookup(name));
            }
        } catch (RemoteException e) {
            System.out.println("failed to find remote object: " + e.getMessage());
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e){
            e.printStackTrace();
        }
    }

    public List<String> getEdge(){
        List<byte[]> results = new ArrayList<byte[]>(5);
        List<String> successes = new ArrayList<String>(5);
        for (RemoteDataNode node: datanodes){
                try
                {
                    results.add(node.getEdge());
                    successes.add(node.toString());
                } catch (RemoteException e)
                {
                    e.printStackTrace();
                }
        }
        
        return successes;
    }
    
    public static void main(String[] args){
        APIServer api = new APIServer(args);
        List<String> successes = api.getEdge();
        
        for (String name : successes)
            System.out.println(name);

      DirectHash dh = new DirectHash(successes.size());
      Set<Node> nodes = dh.getReplicaSetForEdgeQuery(new Edge(new Vertex(0), new Vertex(0)));
      List<Set<Node>> othernodes = dh.getReplicaSetForVertexQuery(new Vertex(0));
      for (Node n: nodes){
        System.out.println(n.toString());
      }

      System.out.println("done with edge query");

      for (Set<Node> set: othernodes){
        for (Node n: set){
          System.out.println(n.toString());
        }
        System.out.println("done with set");
      }
      System.out.println("done with list of sets (vertex query)");
    }
}
    