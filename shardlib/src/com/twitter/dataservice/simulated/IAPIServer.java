package com.twitter.dataservice.simulated;

import java.util.Collection;
import java.util.List;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public interface IAPIServer
{
    public Edge getEdge(Vertex v, Vertex w);
    
    public List<Vertex> getFanout(Vertex v, int pageSize, int offset); 
    
    public List<Vertex> getIntersection(Vertex v, Vertex w, int pageSize, int offset);
    
    public void putEdge(Edge e);
    
    public void putFanout(int vertexid, int[] fanouts);
}
