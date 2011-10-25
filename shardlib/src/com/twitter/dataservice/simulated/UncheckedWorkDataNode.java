package com.twitter.dataservice.simulated;

import com.twitter.dataservice.remotes.IUncheckedWorkDataNode;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UncheckedWorkDataNode extends UnicastRemoteObject implements IUncheckedWorkDataNode 
{
    String name;
    //has a: executor for incoming workTasks
    ExecutorService workExecutor;
    
    public UncheckedWorkDataNode(int numWorkers, String name) throws RemoteException {
        this.name = name;
        workExecutor = Executors.newFixedThreadPool(numWorkers);
    }

    public byte[] getEdge() throws RemoteException
    {
        workExecutor.execute(new WorkTask(1));
        return new byte[]{0};
    }

    public byte[] getIntersection(Integer workFactorLeft, Integer workFactorRight, float intersectionFactor)
            throws RemoteException
    {
        //TODO: make the byte array larger
        workExecutor.execute(new WorkTask(workFactorLeft*workFactorRight));
        return new byte[]{0};
    }

    public byte[] getNeighbors(Integer workFactor) throws RemoteException
    {
      workExecutor.execute(new WorkTask(workFactor));
      System.out.println("executing order...");
      return new byte[]{0};
    }
    

    
}