package com.twitter.dataservice.simulated;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import com.twitter.dataservice.remotes.IDataNode;

public class WorkNodeMain
{
    public static void main(String[] argv){
        String name  = argv[0];
        
        if (System.getSecurityManager() == null)
            System.setSecurityManager(new SecurityManager());
        
        try{
            System.out.printf("Registering work node %s...\n", name);
            IDataNode dn = (IDataNode) new CounterBackedWorkNode();
            IDataNode stub =
                (IDataNode) UnicastRemoteObject.exportObject(dn, 0);
            
            Registry registry = LocateRegistry.createRegistry(1099);
            
            registry.rebind(name, stub);

            //this message is looked for by a test script, don't change.
            System.out.println("success");
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }

    }
}