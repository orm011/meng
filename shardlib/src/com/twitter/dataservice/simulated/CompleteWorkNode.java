package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.twitter.dataservice.remotes.ICompleteWorkNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public class CompleteWorkNode  extends UnicastRemoteObject implements ICompleteWorkNode
{   
    //TODO: will need to keep a dictionary or so to store data
    protected CompleteWorkNode() throws RemoteException
    {
        super();
        // TODO Auto-generated constructor stub
    }

    @Override
    public Edge getEdge(Vertex left, Vertex right) throws RemoteException
    {
        System.out.println("Edge request");
        //TODO: add a payload to edges.
        return new Edge(left, right);
    }

    @Override
    public Collection<Vertex> getFanOut(Vertex v) throws RemoteException
    {
        System.out.println("FanOut request");
        Collection<Vertex> answer = new ArrayList<Vertex>(2);
        answer.add(new Vertex(1));
        answer.add(new Vertex(2));
        
        return answer;
    }

    @Override
    public List<Vertex> getIntersection(Vertex v, Vertex w) throws RemoteException
    {
        System.out.println("intersection request");
        
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void putEdge(Edge e) throws RemoteException
    {
        // TODO Auto-generated method stub
        System.out.println("put request");
        System.out.println(e);
    }

}
