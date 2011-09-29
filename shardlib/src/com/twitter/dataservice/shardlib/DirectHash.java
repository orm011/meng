package com.twitter.dataservice.shardlib;

import com.twitter.dataservice.sharding.IKeyToNode;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

//one of the most basic types of hashing
//hash by left vertex
public class DirectHash implements IKeyToNode
{

    private int numNodes;
    private MessageDigest md;
    
    public DirectHash(int numNodes){
        this.numNodes = numNodes;
      
        try{
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e){
            throw new RuntimeException(e);
        }
    }
    
  public Set<Node> getReplicaSetForEdgeQuery(Edge e) {
    List<Set<Node>> lst = getReplicaSetForVertexQuery(e.getLeftEndpoint());
       return lst.get(0);
  }

    public List<Set<Node>> getReplicaSetForVertexQuery(Vertex v)
    {
        byte[] leftVertexDigest = md.digest(v.toByteArray());
        int num = (((int) leftVertexDigest[0]) & 0xff) % numNodes;
      
        Set<Node> aset = new TreeSet<Node>();
        aset.add(new Node(num));
        List<Set<Node>> ans;
        ans = new LinkedList<Set<Node>>();

        ans.add(aset);
        return ans;
    }
}