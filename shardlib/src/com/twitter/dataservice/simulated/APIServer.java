package com.twitter.dataservice.simulated;

import com.twitter.dataservice.remotes.RemoteDataNode;
import com.twitter.dataservice.sharding.IShardLib;
import com.twitter.dataservice.sharding.RoundRobinShardLib;
import com.twitter.dataservice.sharding.ShardLib;
import com.twitter.dataservice.sharding.TwoTierHashSharding;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class APIServer
{

    Map<Node, RemoteDataNode> nodes = new HashMap<Node, RemoteDataNode>();
    private IShardLib shardinglib = null; // see constructor
    private ExecutorService internalExecutor = Executors.newCachedThreadPool();

    
    public APIServer(String[] dataNodeNames) {
        try {
            for (String name: dataNodeNames){
                System.out.printf("looking up node %s\n", name);
              
                RemoteDataNode remote = (RemoteDataNode) Naming.lookup(name);
                int id = Integer.parseInt(name.substring(name.split("[0-9]+", 0)[0].length(), name
                    .length()));
                Node local = new Node(id);
                nodes.put(local, remote);
            }

            shardinglib = new ShardLib(new TwoTierHashSharding(new ArrayList<Vertex>(), new ArrayList<Node>(nodes.keySet()), 5, 0, 0), null);
            //shardinglib  = new RoundRobinShardLib(nodes.size());
        } catch (RemoteException e) {
            System.out.println("failed to find remote node: " + e.getMessage());
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e){
            e.printStackTrace();
        }
    }


    public byte[] getEdge(Edge e){
        System.out.println("hello");
        Node destination = shardinglib.getNode(e);
        byte[] result = null;

        try {
          result = nodes.get(destination).getEdge();
        } catch(RemoteException re){
          throw new RuntimeException(re);
        }

        return result;
    }

    public List<byte[]> getAllEdges(Vertex v) {
      Collection<Node> destinations = shardinglib.getNodes(v);
      List<byte[]> ans = new LinkedList<byte[]>();

      try{
        //TODO: make calls parallel
        for (Node n : destinations){
            System.out.println(n);
          ans.add(nodes.get(n).getNeighbors(v.getWorkFactor() / destinations.size()));
        }
        
      } catch (RemoteException re){
        throw new RuntimeException(re);
      }

      return ans;
    }
    
    public static void main(String[] args){
        APIServer api = new APIServer(args);
        new Benchmark(api).run();
    }
}