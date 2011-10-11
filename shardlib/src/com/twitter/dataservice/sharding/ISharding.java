package com.twitter.dataservice.sharding;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Pair;
import com.twitter.dataservice.shardutils.Shard;
import com.twitter.dataservice.shardutils.Vertex;

import java.util.Collection;
import java.util.List;


//this is for the virtual shard part. See IKeyToNode for the full path between
//having a vertex/edge and getting the right nodes to query
public interface ISharding {

  public List<Pair<Shard, Collection<Node>>> getShardForVertexQuery(Vertex v);

  //for a given edge(v1,v2), this shard should be contained in the
  //collection from the vertex query.
  public Pair<Shard, Collection<Node>> getShardForEdgeQuery(Edge e);

  //here i need to support getting all relevant edges given shard.
  //the efficiency of this operation depends on both the representations of the shard
  //and the edge Collection.
  //for example, if the collection is an array sorted in 'token' order and the shard is a set of token
  // endpoints, we can do binary search + scan
  public Collection<Edge> getEdgesWithinShard(Shard sh, Collection<Edge> edges);

}
