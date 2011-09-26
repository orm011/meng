package com.twitter.dataservice.sharding;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.twitter.dataservice.shardlib.*;
import com.twitter.dataservice.shardlib.Node;

public class TwoTierHash implements ISharding
{
   private IShardPrimitives state;
 
   private Set<Token> exceptions; //individual tokens that need 
   private Set<IShard> defaultRing;  //hash ring where we deal with usual case
   private Set<IShard> exceptionRing; //a hash ring where we deal with exceptions
   
   private IClusteredHashFunction exceptionTierHash; //the hash function for exceptions 
   private IHashFunction defaultTierHash; //the hash function for default cases
   private Node SPECIAL_INODE;
   
   
   //we use the state (stored in zookeeper) to encode both the exception
   //set, and the two different rings
   public TwoTierHash(IShardPrimitives state){
       for (IShard sh : state.getShardList()){
           //exceptions are encoded in the state as single token shards
           if (sh.getSize() == 1)
               exceptions.add(sh.getLowerEnd());
           
           //the two rings are encoded by using in the shard set and replica set map
           if (state.getReplicaSet(sh).contains(SPECIAL_INODE)){
               exceptionRing.add(sh);
           } else {
               defaultRing.add(sh);
           }
       }
   }
   
//   public TwoTierHash(IImprovedShardPrimitives state){
//       for (){
//           
//       }
//       
//   }
//                   
    public Set<Node> getReplicaSetForEdgeQuery(Edge edge)
    {
        //the default is to hash an edge by left vertex (so all edges for that vertex will be in same shard)
        Token hashval = defaultTierHash.hash(edge);
        Token realhash;
        IShard shard;
        //for exceptions, we hash differently, so that edges for that vertex are spread
        if (exceptions.contains(hashval)) {
            realhash = exceptionTierHash.hash(edge); 
            shard = findShard(exceptionRing, realhash);
        } else {
            shard = findShard(defaultRing, hashval);
        }
        
        return state.getReplicaSet(shard);
    }
    
    public List<Set<Node>> getReplicaSetForVertexQuery(Vertex vertex)
    {
        Token hashval = defaultTierHash.hash(new Edge(vertex, null));
        List<Set<Node>> answer = new ArrayList<Set<Node>>();
        if (exceptions.contains(hashval)){
            Pair<Token, Token> realHashRange = exceptionTierHash.apply(vertex);
            Set<IShard> allRelevantShards = findAllShards(exceptionRing, realHashRange);
            for (IShard sh: allRelevantShards){
                answer.add(state.getReplicaSet(sh));
            }
        } else {
            answer.add(getReplicaSetForEdgeQuery(new Edge(vertex,null)));
        }
        
        return answer;
    }
        
    //find appropriate shard given the shards and a token
    private IShard findShard(Set<IShard> shards, Token hashval){
        return null;
    }
 
    private Set<IShard> findAllShards(Set<IShard> shards, Pair<Token, Token> endToken){
        return null;
    }
        
    public void promoteToException(Vertex v)
    {
        //must do the work of updating shard list, replica map.
    }
    
    public void demoteToDefault(Vertex v)
    {
        //must first bring shards for the same vertex together to the same node (by changing replica map)
        //then must change shardlist to remove unnecessary replicas (from exception list as well as the other list)
    }           
}
