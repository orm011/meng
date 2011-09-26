package com.twitter.dataservice.sharding;
import java.util.List;
import java.util.Set;

import com.twitter.dataservice.shardlib.Edge;
import com.twitter.dataservice.shardlib.Node;
import com.twitter.dataservice.shardlib.Vertex;

public interface ISharding
{
    
    public Set<Node> getReplicaSetForEdgeQuery(Edge e);
    
    public List<Set<Node>> getReplicaSetForVertexQuery(Vertex v);
}