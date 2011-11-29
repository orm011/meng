/**
 * 
 */
package com.twitter.dataservice.simulated.parameters;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
/*
 * name is also used in external scripts, don't change
 */
public class WorkloadParameters extends AbstractParameters {
    public int getNumberOfQueries(){
        return numberOfQueries;
    }

    public double getCorrelationWithGraph(){
        return correlationWithGraph;
    }

    public double getSkew(){
        return skew;
    }

    public int getPercentEdge(){
        return percentEdge;
    }
    
    public int getPercentVertex(){
        return percentVertex;
    }

    public int getPercentIntersection(){
        return percentIntersection;
    }

    private int numberOfQueries;
    private double correlationWithGraph;
    private double skew;
    private int percentEdge;
    private int percentVertex;
    private int percentIntersection;
    
    public List<Map.Entry<String,Object>> fields() {
        Map<String,Object> stuff = new LinkedHashMap<String,Object>();
        stuff.put("numberOfQueries", numberOfQueries);
        stuff.put("querySkew", skew);
        stuff.put("skewCorrelationWithGraph", correlationWithGraph);
        stuff.put("percentEdge", percentEdge);
        stuff.put("percentVertex", percentVertex);
        stuff.put("percentIntersection", percentIntersection);
        
        return new LinkedList<Map.Entry<String,Object>>(stuff.entrySet());
    }
    
                    
    public static class Builder {
        
        private int numberOfQueries = 0;
        private double correlationWithGraph = 0;
        private double skew = 1;
        private int percentEdge = 50;
        private int percentVertex = 50;
        private int percentIntersection = 0;
     
        public WorkloadParameters.Builder numberOfQueries(int nq){
            numberOfQueries = nq;
            return this;
        }
        
        public WorkloadParameters.Builder skew(double sk){
            skew = sk;
            return this;
        }
        
        public WorkloadParameters.Builder percentEdge(int pe){
            percentEdge = pe;
            return this;
        }
        
        public WorkloadParameters.Builder percentVertex(int pv){
            percentVertex = pv;
            return this;
        }
        
        public WorkloadParameters.Builder queryTypeDistribution(int percentEdge, int percentVertex, int percentIntersection){
            this.percentEdge = percentEdge;
            this.percentVertex = percentVertex;
            this.percentIntersection = percentIntersection;
            return this;
        }
        
        public WorkloadParameters build(){
            //TODO: add other checks
            if (percentEdge + percentVertex + percentIntersection != 100 || skew <= 0 || 
                    correlationWithGraph > 1 || correlationWithGraph < 0 ||
                    numberOfQueries < 0){
                throw new IllegalArgumentException();
            }
            
            WorkloadParameters ans = new WorkloadParameters();
            ans.numberOfQueries = this.numberOfQueries;
            ans.correlationWithGraph = this.correlationWithGraph;
            ans.skew = this.skew;
            ans.percentEdge = this.percentEdge;
            ans.percentIntersection = this.percentIntersection;
            ans.percentVertex = this.percentVertex;
            
            return ans;
        }   
    }
}