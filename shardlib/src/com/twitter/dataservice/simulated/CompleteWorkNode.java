package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.twitter.dataservice.remotes.ICompleteWorkNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public class CompleteWorkNode  extends UnicastRemoteObject implements ICompleteWorkNode
{   
    //TODO: will need to keep a dictionary or so to store data
    Map<Vertex, Integer> internalCount = new HashMap<Vertex, Integer>();
    
    protected CompleteWorkNode() throws RemoteException
    {
        super();
    }

    @Override
    public Edge getEdge(Vertex left, Vertex right) throws RemoteException
    {
        //we don't check if it is there, to not have to keep track.
        //we assume the shardling lib did a good job of directing the request.
        //edges have a payload, check parameters in systemparams.
        assert internalCount.containsKey(left);
        System.out.println("Edge request");
        return new Edge(left, right);
    }

    @Override
    public Collection<Vertex> getFanOut(Vertex v) throws RemoteException
    {
        System.out.println("FanOut request");
        
        assert internalCount.containsKey(v);
        ArrayList<Vertex> answer = new ArrayList<Vertex>(internalCount.get(v));        
        for (int i = 0; i < answer.size(); i++){
            answer.add(i, new Vertex(i));
        }
        
        return answer;
    }

    @Override
    public List<Vertex> getIntersection(Vertex v, Vertex w) throws RemoteException
    {
        System.out.println("Intersection request");
        throw new UnsupportedOperationException();
    }

    @Override
    public void putEdge(Edge e) throws RemoteException
    {
        System.out.println("put request");
        Integer prev;
        if ((prev = internalCount.get(e.getLeftEndpoint())) == null) internalCount.put(e.getLeftEndpoint(), 1);
        else internalCount.put(e.getRightEndpoint(), prev + 1);
    }
}