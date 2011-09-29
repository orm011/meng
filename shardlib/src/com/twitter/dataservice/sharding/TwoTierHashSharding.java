package com.twitter.dataservice.sharding;

import com.twitter.dataservice.shardlib.Edge;
import com.twitter.dataservice.shardlib.Pair;
import com.twitter.dataservice.shardlib.Shard;
import com.twitter.dataservice.shardlib.Vertex;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;

//most basic sharding. edge goes to  a Token.
public class TwoTierHashSharding implements ISharding
{
  private MessageDigest hashfun = null;
  Collection<Shard> shards;
  Collection<Pair<Vertex,Integer>> exceptions;
  //the exceptions are also implicit in the shards collection.
  //basically, at query time we notice oversized vertices.

  public TwoTierHashSharding(){
        try{
            hashfun = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e){
            throw new RuntimeException(e);
        }
    }

  @Override public Shard getShardForEdgeQuery(Edge e) {
        // hash edge to md5(v),md5(w)
        // look within shardlist for an individual shard where it fits.
        throw new NotImplementedException();
  }

  @Override public List<Shard> getShardForVertexQuery(Vertex v) {
        //endpoints are md5(v),00000 - md5(v),111111.. find all overlapping shards in the list
        throw new NotImplementedException();
  }

  @Override public Collection<Edge> getEdgesWithinShard(Shard sh, Collection<Edge> edges) {
        //to be executed at data nodes.
    // for my two tier thing could work by looking at ranges. and binary search.
        throw new NotImplementedException();
  }

  void updateVertexCategory(Vertex v, Integer count){
    //look at the exceptions list, and the count,
    //and see if we should promote it this vertex based on that
    //this may involve adding an exception, or updating an entry already in the exceptions list, or
    // removing one
  }

  
}
