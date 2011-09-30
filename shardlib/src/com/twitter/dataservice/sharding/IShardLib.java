package com.twitter.dataservice.sharding;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;

import java.util.Collection;

public interface IShardLib {

    public Node getNode(Edge e);
    public Collection<Node> getNodes(Vertex v);
  
}
