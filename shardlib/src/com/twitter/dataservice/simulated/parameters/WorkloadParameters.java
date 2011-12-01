/**
 * 
 */
package com.twitter.dataservice.simulated.parameters;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
/*
 * class name is also used in external scripts. don't change
 */
public class WorkloadParameters extends AbstractParameters {
    
    public static final String NUMBER_OF_QUERIES = "workload.numberOfQueries";
    public static final String PERCENT_EDGE_QUERIES = "workload.percentEdge";
    public static final String PERCENT_FANOUT_QUERIES = "workload.percentFanout";
    public static final String PERCENT_INTERSECTION_QUERIES = "workload.percentIntersection";
    public static final String QUERY_SKEW = "workload.querySkew";
    
    public List<Map.Entry<String,Object>> fields() {
        Map<String,Object> stuff = new LinkedHashMap<String,Object>();
        stuff.put(NUMBER_OF_QUERIES, numberOfQueries);
        stuff.put(QUERY_SKEW, skew);
        stuff.put(PERCENT_EDGE_QUERIES, percentEdge);
        stuff.put(PERCENT_FANOUT_QUERIES, percentVertex);
        stuff.put(PERCENT_INTERSECTION_QUERIES, percentIntersection);
        
        //stuff.put();
        return new LinkedList<Map.Entry<String,Object>>(stuff.entrySet());
    }
    
    private int numberOfQueries;
    private double skew;
    private int percentEdge;
    private int percentVertex;
    private int percentIntersection;
    
    
    public int getNumberOfQueries(){
        return numberOfQueries;
    }

    public double getQuerySkew(){
        return skew;
    }

    public int getPercentEdgeQueries(){
        return percentEdge;
    }
    
    public int getPercentVertexQueries(){
        return percentVertex;
    }

    public int getPercentIntersectionQueries(){
        return percentIntersection;
    }
    
                    
    public static class Builder {
        
        private int numberOfQueries = 0;
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
        //TODO: figure out how to deal with defaults
        public WorkloadParameters.Builder queryTypeDistribution(int percentEdge, int percentVertex, int percentIntersection){
            this.percentEdge = percentEdge;
            this.percentVertex = percentVertex;
            this.percentIntersection = percentIntersection;
            return this;
        }
        
        public WorkloadParameters build(){
            //TODO: add other checks
            if (percentEdge + percentVertex + percentIntersection != 100 || skew <= 0 || 
                    numberOfQueries < 0){
                throw new IllegalArgumentException();
            }
            
            WorkloadParameters ans = new WorkloadParameters();
            ans.numberOfQueries = this.numberOfQueries;
            ans.skew = this.skew;
            ans.percentEdge = this.percentEdge;
            ans.percentIntersection = this.percentIntersection;
            ans.percentVertex = this.percentVertex;
            
            return ans;
        }   
    }
}