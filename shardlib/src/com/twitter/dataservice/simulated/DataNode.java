package com.twitter.dataservice.simulated;

import com.twitter.dataservice.remotes.RemoteDataNode;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataNode extends UnicastRemoteObject implements RemoteDataNode 
{
    String name;
    //has a: executor for incoming workTasks
    ExecutorService workExecutor;
    
    public DataNode(int numWorkers, String name) throws RemoteException {
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
    
    public static void main(String[] argv){

        String name  = argv[0];
        if (System.getSecurityManager() == null)
            System.setSecurityManager(new RMISecurityManager());
        
        try{
            System.out.printf("Registering work node %s...\n", name);
            DataNode dn = new DataNode(SystemParameters.workersPerNode, name);
            Naming.rebind(dn.name, dn);

            //this message is looked for by a test script, don't change.
            System.out.println("success");
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }

    }
    
}