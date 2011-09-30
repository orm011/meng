package com.twitter.dataservice.sharding;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

//one of the most basic types of hashing
//hash by left vertex on id
public class RoundRobinShardLib implements IShardLib
{

    private int numNodes;
    
    public RoundRobinShardLib(int numNodes){
        this.numNodes = numNodes;
    }
   
  @Override public Node getNode(Edge e) {
    LinkedList<Node> lst = (LinkedList<Node>) getNodes(e.getLeftEndpoint());
    return lst.get(0);
  }

  @Override public Collection<Node> getNodes(Vertex v) {
         int num = v.getId() % numNodes;
         List<Node> ans = new LinkedList<Node>();
         ans.add(new Node(num));
         return ans;
  }
}