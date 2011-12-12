package com.twitter.dataservice.simulated;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import com.twitter.dataservice.remotes.IDataNode;

public class WorkNodeMain
{
    public static void main(String[] argv){
        
        if (argv.length == 0){
            System.out.println("usage: NodeName RegistryPort");
            System.exit(1);    
        }
        
        if (System.getSecurityManager() == null)
            System.setSecurityManager(new SecurityManager());
        
        try {            
            Registry registry;
            registry = LocateRegistry.createRegistry(1099);
            
            for (String name: argv){
                System.out.printf("Registering work node %s...\n", name);
                IDataNode dn = new DictionaryBackedDataNode(name);
                IDataNode stub =
                    (IDataNode) UnicastRemoteObject.exportObject(dn, 0);
                registry.rebind(name, stub);
                System.out.println("Great success.");
            }
            
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }

    }
}