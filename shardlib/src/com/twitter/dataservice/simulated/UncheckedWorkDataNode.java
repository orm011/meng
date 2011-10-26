package com.twitter.dataservice.simulated;

import com.twitter.dataservice.remotes.IUncheckedWorkDataNode;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//used to test RMI. not being used now.
public class UncheckedWorkDataNode extends UnicastRemoteObject implements IUncheckedWorkDataNode 
{
    String name;
    
    public UncheckedWorkDataNode(int numWorkers, String name) throws RemoteException {
        this.name = name;
    }

    public byte[] getEdge() throws RemoteException
    {
        try {Thread.sleep(100); } catch (InterruptedException e) {System.out.println("sleep problem");}
        System.out.println(this.name + " done");
        return new byte[]{0};
    }

    public byte[] getIntersection(Integer workFactorLeft, Integer workFactorRight, float intersectionFactor)
            throws RemoteException
    {
        //TODO: make the byte array larger
        return new byte[]{0};
    }

    public byte[] getNeighbors(Integer workFactor) throws RemoteException
    {
        try {Thread.sleep(100);} catch (InterruptedException e) { System.out.println("sleep problem");}
        System.out.println(this.name + "executing order...");
        return new byte[]{0};
    }
    

    
}