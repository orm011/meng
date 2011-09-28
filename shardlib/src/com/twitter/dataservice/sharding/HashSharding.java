package com.twitter.dataservice.sharding;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;

import com.twitter.dataservice.remotes.RemoteDataNode;
import com.twitter.dataservice.shardlib.Edge;
import com.twitter.dataservice.shardlib.Node;
import com.twitter.dataservice.shardlib.Vertex;

//most basic sharding. edge goes to  a Token.
public class HashSharding implements ISharding
{
    //may want to have more than this state only?
    List<RemoteDataNode> livenodes;
    private MessageDigest hashfun;
    
    public HashSharding(List<RemoteDataNode> nodes){
        try{
            hashfun = MessageDigest.getInstance("SHA");
        } catch (NoSuchAlgorithmException e){
            throw new RuntimeException(e);
        }        
        
        livenodes = nodes;
    }
    
       
    
    @Override
    public Set<Node> getReplicaSetForEdgeQuery(Edge e)
    {
        return null;
    }

    @Override
    public List<Set<Node>> getReplicaSetForVertexQuery(Vertex v)
    {
        return null;   
    }

}
