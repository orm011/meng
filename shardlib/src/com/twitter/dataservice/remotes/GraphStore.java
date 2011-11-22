package com.twitter.dataservice.remotes;

import java.util.Collection;
import java.util.List;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

//TODO: make work nodes and API server both implement this?
public interface GraphStore
{
    Edge getEdge(Vertex left, Vertex right);
    
    Collection<Vertex> getFanout(Vertex v);
    
    List<Vertex> getIntersection(Vertex v, Vertex w);    
    
    void putEdge(Vertex v, Vertex w);
}
