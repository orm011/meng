package com.twitter.dataservice.remotes;

import java.rmi.RemoteException;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public interface ICheckedFanOutNode
    {        
        Boolean getEdge(Edge e) throws java.rmi.RemoteException;

        //returns the count of edges for that vertex.
        //TODO: make this do work and send data elsewhere.
        Integer getNeighbors(Vertex v) throws java.rmi.RemoteException;
        
        void putEdge(Edge e) throws RemoteException;
    }