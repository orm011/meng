package com.twitter.dataservice.simulated;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.parameters.AbstractParameters;
import com.twitter.dataservice.parameters.GraphParameters;
import com.twitter.dataservice.parameters.SystemParameters;
import com.twitter.dataservice.parameters.WorkloadParameters;

public abstract class BasicCollector implements MetricsCollector
{
    final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);
    private GraphParameters gp;
    private WorkloadParameters wp;
    
    public BasicCollector(GraphParameters graphParams, WorkloadParameters workloadParams){
        gp = graphParams;
        wp = workloadParams;
    }
    
    @Override
    public void begin()
    {
        Logger paramslogger = org.slf4j.LoggerFactory.getLogger(AbstractParameters.class);
        paramslogger.debug("{}", SystemParameters.instance());
        paramslogger.debug("{}", gp);
        paramslogger.debug("{}", wp);
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
