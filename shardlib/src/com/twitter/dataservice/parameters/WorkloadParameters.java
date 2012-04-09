/**
 * 
 */
package com.twitter.dataservice.parameters;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.twitter.dataservice.parameters.SamplableBuilder.DistributionType;
import com.twitter.dataservice.simulated.Samplable;
/*
 * class name is also used in external scripts. don't change
 */
public class WorkloadParameters extends AbstractParameters {
    
	public static final String prefix = "workload";
    public static final String NUMBER_OF_QUERIES = "workload.numberOfQueries";
    public static final String PERCENT_EDGE_QUERIES = "workload.percentEdge";
    public static final String PERCENT_FANOUT_QUERIES = "workload.percentFanout";
    public static final String PERCENT_INTERSECTION_QUERIES = "workload.percentIntersection";
    public static final String DISTRIBUTION_TYPE = "workload.distributionType";
    
    public List<Map.Entry<String,Object>> fields() {
        Map<String,Object> stuff = new LinkedHashMap<String,Object>();
        stuff.put(NUMBER_OF_QUERIES, numberOfQueries);
        stuff.put(PERCENT_EDGE_QUERIES, percentEdge);
        stuff.put(PERCENT_FANOUT_QUERIES, percentVertex);
        stuff.put(PERCENT_INTERSECTION_QUERIES, percentIntersection);
        
        //stuff.put();
        return new LinkedList<Map.Entry<String,Object>>(stuff.entrySet());
    }
    
    final private int numberOfQueries;
    final private int percentEdge;
    final private int percentVertex;
    final private int percentIntersection;
    final private Samplable rand;

    
    public WorkloadParameters(int numberOfQueries, int percentEdge,
			int percentVertex, int percentIntersection, Samplable rand) {
		super();
		if (!(percentEdge + percentVertex + percentIntersection == 100)) throw new IllegalArgumentException();
		this.numberOfQueries = numberOfQueries;
		this.percentEdge = percentEdge;
		this.percentVertex = percentVertex;
		this.percentIntersection = percentIntersection;
		this.rand = rand;
	}

	public Samplable getSamplable() {
		return rand;
	}

	public int getNumberOfQueries(){
        return numberOfQueries;
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
        
        /*
         * by default, numVertices and numBins are the same, but
         * for better performance can make them different
         */
        public WorkloadParameters build(int numVertices){
        	return build(numVertices, numVertices);
        }
        
        public WorkloadParameters build(int numVertices, int resolution){
            //TODO: add other checks
            if (percentEdge + percentVertex + percentIntersection != 100 || skew <= 0 || 
                    numberOfQueries < 0){
                throw new IllegalArgumentException();
            }
            
            SamplableBuilder rand = new SamplableBuilder();
            rand.setType(DistributionType.ZIPF);
            rand.setLowLimit(0);
            rand.setHighLimit(numVertices);
            rand.setSkew((float)this.skew);
            rand.setZipfbins(resolution);
           
            WorkloadParameters ans = new WorkloadParameters(numberOfQueries, percentEdge, 
            		percentVertex, percentIntersection, rand.build());
            
            return ans;
        }   
    }
}