package com.twitter.dataservice.remotes;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public interface IDataNode extends Remote
{
    Edge getEdge(Vertex left, Vertex right) throws  RemoteException;
    
    int[] getFanout(Vertex v, int pageSize, int offset) throws RemoteException;

    Collection<Vertex> getFanout(Vertex v) throws RemoteException;
    
    int[] getIntersection(Vertex v, Vertex w, int pageSize, int offset) throws RemoteException;
    
    void putFanout(int vertex, int[] fanout) throws RemoteException;
    
    void putEdge(Edge e) throws RemoteException;
    
    //Random walk. Need to get to this later. <- haha forget it bro.    
    //some utilities
    void reset() throws RemoteException;
    int totalLoad() throws RemoteException;
}
