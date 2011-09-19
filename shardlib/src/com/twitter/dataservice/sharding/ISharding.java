package com.twitter.dataservice.sharding;
import java.util.List;
import java.util.Set;

import com.twitter.dataservice.shardlib.Edge;
import com.twitter.dataservice.shardlib.IKey;
import com.twitter.dataservice.shardlib.INode;
import com.twitter.dataservice.shardlib.Vertex;

public interface ISharding
{
    
    public Set<INode> getReplicaSetForEdgeQuery(Edge e);
    
    public List<Set<INode>> getReplicaSetForVertexQuery(Vertex v);    
}
