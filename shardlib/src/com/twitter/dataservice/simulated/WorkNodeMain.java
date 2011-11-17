package com.twitter.dataservice.simulated;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.Remote;

public class WorkNodeMain
{
    public static void main(String[] argv){
        String name  = argv[0];
        if (System.getSecurityManager() == null)
            System.setSecurityManager(new RMISecurityManager());
        
        try{
            System.out.printf("Registering work node %s...\n", name);
            Remote dn = (Remote) new CounterBackedWorkNode();
            Naming.rebind(name, dn);

            //this message is looked for by a test script, don't change.
            System.out.println("success");
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }

    }
}
