package com.twitter.dataservice.sharding;

import com.twitter.dataservice.shardlib.Edge;
import com.twitter.dataservice.shardlib.Shard;
import com.twitter.dataservice.shardlib.Vertex;

import java.util.Collection;

public interface ISharding {

  public Collection<Shard> getShardForVertexQuery(Vertex v);

  //for a given edge(v1,v2), this shard should be contained in the
  //collection from the vertex query.
  public Shard getShardForEdgeQuery(Edge e);

  //here i need to support getting all relevant edges given shard.
  //the efficiency of this operation depends on both the representations of the shard
  //and the edge Collection.
  //for example, if the collection is an array sorted in 'token' order and the shard is a set of token
  // endpoints, we can do binary search + scan
  public Collection<Edge> getEdgesWithinShard(Shard sh, Collection<Edge> edges);

}
