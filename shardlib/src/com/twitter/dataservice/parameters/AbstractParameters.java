package com.twitter.dataservice.parameters;

import java.util.List;
import java.util.Map;

public abstract class AbstractParameters
{
    public abstract List<Map.Entry<String, Object>> fields();
    
    @Override public String toString(){
        List<Map.Entry<String, Object>> values = fields();
        StringBuilder sb = new StringBuilder(values.size()*15);
        
        sb.append(this.getClass().getSimpleName());
        sb.append(" ");
        
        for (Map.Entry<String, Object> entry : values){
            sb.append(entry.getKey());
            sb.append(" ");
            sb.append(entry.getValue());
            sb.append(" ");
        }
        
        return sb.toString();   
    }
}
