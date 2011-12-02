package com.twitter.dataservice.remotes;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public interface ICompleteWorkNode extends Remote
{
    Edge getEdge(Vertex left, Vertex right) throws  RemoteException;
    
    Collection<Vertex> getFanout(Vertex v) throws RemoteException;
    
    List<Vertex> getIntersection(Vertex v, Vertex w) throws RemoteException;
    
    //Random walk. Need to get to this later.
    
    void putEdge(Edge e) throws RemoteException;
    
    //some utilities
    void reset() throws RemoteException;
    int totalLoad() throws RemoteException;
}
