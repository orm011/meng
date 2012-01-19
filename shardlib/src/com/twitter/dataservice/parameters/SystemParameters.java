package com.twitter.dataservice.parameters;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


//parameters that are not meant to be changed in as we change workloads, 
//but which may need to be changed before settling on a final value.
public class SystemParameters extends AbstractParameters
{
    
    public static final String NUM_DATA_NODES = "system.numDataNodes";
    public static final String PER_EDGE_WEIGHT = "system.perEdgeWeight";
    
    public static final int DEFAULT_EDGE_WEIGHT = 1; //No space for more
    public static final int DEFAULT_NUMBER_NODES = 1;
    
    private static SystemParameters instance = new SystemParameters(DEFAULT_EDGE_WEIGHT, DEFAULT_NUMBER_NODES);
    
    //is this a good idea?
    //may confuse the two
    public static void reset(int edgeSize, int numWorkNodes){        
        instance = new SystemParameters(edgeSize, numWorkNodes);
    }
    
    public final int perEdgeWeight;
    public int numDataNodes;
    
    //TODO: initialize from config file
    private SystemParameters(int edgeSize, int numWorkNodes){
        if (edgeSize <= 0 || numWorkNodes <=0) throw new IllegalArgumentException();
        this.perEdgeWeight = edgeSize;
        this.numDataNodes = numWorkNodes;
    }
    
    public static SystemParameters instance(){
        return instance;
    }
    
    public  List<Map.Entry<String,Object>> fields(){
        HashMap<String, Object> ans = new HashMap<String, Object>();
        ans.put(PER_EDGE_WEIGHT, perEdgeWeight);
        ans.put(NUM_DATA_NODES, numDataNodes);
        
        return new LinkedList<Map.Entry<String,Object>>(ans.entrySet());
    }
}
