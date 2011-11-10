package com.twitter.dataservice.simulated;

import java.util.Collection;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public interface IAPIServer
{
    public Edge getEdge(Vertex v, Vertex w);
    
    public Collection<Vertex> getFanout(Vertex v); 
    
    public Collection<Vertex> getIntersection(Vertex v, Vertex w);
}
