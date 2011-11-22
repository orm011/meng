package com.twitter.dataservice.simulated;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.parameters.GraphParameters;
import com.twitter.dataservice.simulated.parameters.SystemParameters;
import com.twitter.dataservice.simulated.parameters.WorkloadParams;

public abstract class BasicCollector implements MetricsCollector
{
    final String prefix;
    final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);
    
    public BasicCollector(GraphParameters graphParams, WorkloadParams workloadParams){
        final List<Map.Entry<String, Object>> logPrefix = new LinkedList<Map.Entry<String, Object>>();

        logPrefix.addAll(SystemParameters.fields());
        logPrefix.addAll(graphParams.fields());
        logPrefix.addAll(workloadParams.fields());
        
        StringBuilder sb = new StringBuilder();
        
        for (Iterator<Map.Entry<String, Object>> it = logPrefix.iterator(); it.hasNext(); ){
            Map.Entry<String, Object> entry = it.next();
            sb.append(entry.getKey());
            sb.append(" ");
            sb.append(entry.getValue());
            sb.append(" ");
        }
        
        this.prefix = sb.toString();
    }
    
    @Override
    public void begin()
    {
        Logger paramslogger = org.slf4j.LoggerFactory.getLogger(WorkloadParams.class);
        paramslogger.debug(prefix);
    }

    @Override
    public void finish()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void logEdge(long timeStart, long timeEnd, Vertex left, Vertex right, boolean answer)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void logFanout(long timeStart, long timeEnd, Vertex arg, int answer)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void logIntersection(long timeStart, long timeEnd, Vertex arg1, Vertex arg2, int answer)
    {
        // TODO Auto-generated method stub

    }

}
