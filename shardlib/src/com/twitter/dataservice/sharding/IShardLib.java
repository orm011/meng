package com.twitter.dataservice.sharding;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;

import java.util.Collection;

/*
 * Basic sharding lib interface. This is what the APIServer or
 * client libs should use. 
 * 
 * 
 */
public interface IShardLib {

    public Node getNode(Edge e);
    public Collection<Node> getNodes(Vertex v);
  
}