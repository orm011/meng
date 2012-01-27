package com.twitter.dataservice.simulated;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.LinkedList;
import java.util.List;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public class WorkNodeMain
{
    public static void main(String[] argv){
        
        if (argv.length == 0){
            System.out.println("usage: NodeName-[loadFile] [NodeName-[loadFile]...] ");
            System.exit(1);    
        }
        
        if (System.getSecurityManager() == null)
            System.setSecurityManager(new SecurityManager());
        
        try {            
            Registry registry;
            registry = LocateRegistry.createRegistry(1099);
            
            List<IDataNode> nodes = new LinkedList<IDataNode>();
            
            for (String namefile: argv){
                String name = namefile.split("-")[0];
                
                System.out.printf("Registering work node %s...\n", name);
                CompactDataNode dn = new CompactDataNode();
                nodes.add(dn);
                IDataNode stub =
                    (IDataNode) UnicastRemoteObject.exportObject(dn, 0);
                registry.rebind(name, stub);
                
                if (namefile.split("-").length > 1){
                    String filename = namefile.split("-")[1];    
                    System.out.println(String.format("loading %s using local file %s", name, filename));
                    loadFromLocal(filename, dn);
                }
                
                System.out.println("Success");
            }
            
//            while (true) {
//                Thread.sleep(5000);
//                System.out.println("sleeping..");
//            }
//            
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
  }
    
    /*
     * only available for CompactDataNode because we put fanout arrays directly into node.
     */
   public static void loadFromLocal(String fileName, CompactDataNode node){
       try
    {
        BufferedReader bfr = new BufferedReader(new FileReader(fileName));
        String line;
        while ((line = bfr.readLine()) != null){
            String[] elts = line.split("\t");
            int leftid = Integer.parseInt(elts[0]);
            
            //System.out.println(leftid);
            //System.out.println(elts[1]);
            String[] fanoutstring= elts[1].split(" ");
            int[] fanouts = new int[fanoutstring.length];
            int i = 0;
            for (String num: fanoutstring){
                fanouts[i] = Integer.parseInt(num);
                i++;
            }

            node.putFanout(leftid, fanouts);
        }  
    } catch (FileNotFoundException e)
    {
        throw new RuntimeException(e);
    } catch (IOException e)
    {
        throw new RuntimeException(e);
    }
    
  }

}