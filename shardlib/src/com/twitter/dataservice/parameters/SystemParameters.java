package com.twitter.dataservice.parameters;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


//parameters that are not meant to be changed in as we change workloads, 
//but which may need to be changed before settling on a final value.
public class SystemParameters extends AbstractParameters
{
    public final int edgespace = 100; 
    public final int workNodes = 1;
    
    //TODO: initialize from config file
    private static SystemParameters instance = new SystemParameters();
       
    public static SystemParameters instance(){
        return instance;
    }
    
    public  List<Map.Entry<String,Object>> fields(){
        HashMap<String, Object> ans = new HashMap<String, Object>();
        ans.put("edgePayload", edgespace);
        ans.put("workNodes", workNodes);
        
        return new LinkedList<Map.Entry<String,Object>>(ans.entrySet());
    }
    
}
