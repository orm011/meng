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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class APIServer
{

    Map<Node, RemoteDataNode> nodes = new HashMap<Node, RemoteDataNode>();
    private ISharding shardinglib = null; // see constructor
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

            shardinglib  = new DirectHash(nodes.size());
        } catch (RemoteException e) {
            System.out.println("failed to find remote node: " + e.getMessage());
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e){
            e.printStackTrace();
        }
    }

  //this is where you look into state or something
  //like mastership and live nodes
  // to decide which nodes to query from all
  // the options.
  //right now I am picking a single elt of each set. arbitrarily.
    private List<Node> getNodesForQuery(List<Set<Node>> options){
        Iterator<Set<Node>> it = options.iterator();
        List<Node> answer = new LinkedList<Node>();

        assert it.hasNext();//non-empty
        while (it.hasNext()){
          answer.add(getNodeForQuery(it.next()));
        }

        return answer;
    }

    private Node getNodeForQuery(Set<Node> options){
        Iterator<Node> nodeit = options.iterator();
        assert nodeit.hasNext();
        Node answer = nodeit.next();
        return answer;
    }

    public byte[] getEdge(Edge e){
        System.out.println("hello");
        Set<Node> reps = shardinglib.getReplicaSetForEdgeQuery(e);
        Node destination = getNodeForQuery(reps);
        byte[] result = null;

        try {
          result = nodes.get(destination).getEdge();
        } catch(RemoteException re){
          throw new RuntimeException(re);
        }

        return result;
    }

    public List<byte[]> getAllEdges(Vertex v) {
      List<Set<Node>> reps = shardinglib.getReplicaSetForVertexQuery(v);
      List<Node> destinations = getNodesForQuery(reps);
      List<byte[]> ans = new LinkedList<byte[]>();

      try{
        //TODO: make calls parallel
        for (Node n : destinations){
          ans.add(nodes.get(n).getNeighbors(v.getWorkFactor() / destinations.size()));
        }
        
      } catch (RemoteException re){
        throw new RuntimeException(re);
      }

      return ans;
    }
    
    public static void main(String[] args){
        APIServer api = new APIServer(args);
        byte[] res = api.getEdge(new Edge(new Vertex(1,10), new Vertex(2,10)));
        List<byte[]> resgroup = api.getAllEdges(new Vertex(1,30));
    }
}