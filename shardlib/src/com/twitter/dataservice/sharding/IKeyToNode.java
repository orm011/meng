package com.twitter.dataservice.sharding;
import com.twitter.dataservice.shardlib.Edge;
import com.twitter.dataservice.shardlib.Node;
import com.twitter.dataservice.shardlib.Vertex;

import java.util.List;
import java.util.Set;

//this maps edges/vertices to nodes (to distinguish
// it from a sharding, which maps edges/vertices to shards.
//those can then be mapped to nodes.  don't use this.
public interface IKeyToNode
{
    
    public Set<Node> getReplicaSetForEdgeQuery(Edge e);
    
    public List<Set<Node>> getReplicaSetForVertexQuery(Vertex v);
}