package com.twitter.dataservice.simulated.parameters;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


//parameters that are not meant to be changed in as we change workloads, 
//but which may need to be changed before settling on a final value.
public class SystemParameters
{
    public static int edgespace = 50; 
    public static int workNodes = 1;
    
    public static List<Map.Entry<String,Object>> fields(){
        HashMap<String, Object> ans = new HashMap<String, Object>();
        ans.put("edgepayload", edgespace);
        ans.put("workNodes", workNodes);
        
        return new LinkedList<Map.Entry<String,Object>>(ans.entrySet());
    }
}
