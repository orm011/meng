package com.twitter.dataservice.sharding;

import com.twitter.dataservice.shardutils.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;

//most basic sharding. edge goes to  a Token.
public class TwoTierHashSharding implements ISharding
{
  Collection<Shard> shards;
  Collection<Pair<Vertex,Integer>> exceptions;
  HierarchicalHashFunction hashfun = new TwoTierHashSharding.HierarchicalHashFunction();
  //the exceptions are also implicit in the shards collection.
  //basically, at query time we notice oversized vertices.

  public TwoTierHashSharding(){
      
  }

  @Override public Shard getShardForEdgeQuery(Edge e) {
        // hash edge to hash(v),hash(w)
        Token tok = hashfun.hash(e);

        // search within shardlist for an individual shard where it fits.
        // return shard.
        throw new NotImplementedException();
  }

  @Override public List<Shard> getShardForVertexQuery(Vertex v) {
        //endpoints are hash(v),00000 - hash(v),111111.
        Pair<Token,Token> limits = hashfun.hash(v);

        // search within the shard collections all involved.
        throw new NotImplementedException();
  }

  @Override public Collection<Edge> getEdgesWithinShard(Shard sh, Collection<Edge> edges) {
        //to be executed at data nodes.
      // for this ISharding method, it would work by looking at token ranges.
        throw new NotImplementedException();
  }

  void updateVertexCategory(Vertex v, Integer count){
    //look at the exceptions list, and the count,
    //and see if we should promote it this vertex based on that
    //this may involve adding an exception, or updating an entry already in the exceptions list, or
    // removing one
  }

  static class HierarchicalHashFunction implements IHashFunction {
  //by hierarchical we mean there are two levels to it (one per vertex)
  //basically the token for (edge(v1,v2)) is  (hash(v1),hash(v2))
  //this way, all edges for a given node are in a contiguous token range,
  //and I can use the same representation for all kinds of shards

   private static MessageDigest hashfun;

   static {
     try {
      hashfun = MessageDigest.getInstance("MD5");
     } catch (NoSuchAlgorithmException e) {
       throw new RuntimeException(e);
     }
   }

  public Token hash(Edge e) {
    byte[] left = hashfun.digest(e.getLeftEndpoint().toByteArray());
    byte[] right = hashfun.digest(e.getRightEndpoint().toByteArray());
    byte[] hashed = new byte[left.length + right.length];
    System.arraycopy(left, 0, hashed, 0, left.length);
    System.arraycopy(right, 0, hashed, left.length, right.length);

    return new Token(hashed);
  }

  public Pair<Token, Token> hash(Vertex v) {
    byte[] leftend = hashfun.digest(v.toByteArray());
    byte[] high = new byte[leftend.length];
    //TODO: finish later. idea is clear
    return null;// basically the range hash(v1),000...0 to hash(v1),111...1
  }
}



}

