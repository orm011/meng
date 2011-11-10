package com.twitter.dataservice.simulated;

import java.util.Iterator;

import com.sun.org.apache.xml.internal.utils.UnImplNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public interface BenchmarkData
{
    
    Iterator<Edge> graphIterator();
    
    Iterator<Query> workloadIterator(WorkloadParams params);

    
    public static abstract class Query {
        static class EdgeQuery extends Query{
            Vertex left;
            Vertex right;
            
            private EdgeQuery(Vertex v, Vertex w){
                left = v;
                right = w;
            }
            
            @Override
            public void execute(APIServer api){
                api.getEdge(left, right);
            }
        }
        
        static class FanoutQuery extends Query{
            Vertex v;
            
            private FanoutQuery(Vertex v){
                this.v = v;
            }
            
            @Override
            public void execute(APIServer api){
                api.getFanout(v);
            }
        }
        
        static class IntersectionQuery extends Query {
            Vertex left;
            Vertex right;
            
            private IntersectionQuery(Vertex v, Vertex w){
                left = v;
                right = w;
            }
            
            @Override
            public void execute(APIServer api){
                throw new RuntimeException("not implemented yet");
            }

        }
        
        public static Query edgeQuery(Vertex v, Vertex w){
            return new EdgeQuery(v, w);
        }
        
        public static Query fanoutQuery(Vertex v){
            return new FanoutQuery(v);
        }
        
        public static Query intersectionQuery(Vertex v, Vertex w){
            return new IntersectionQuery(v, w);
        }
        
        public abstract void execute(APIServer api);
    }
    
    
    
    public static class WorkloadParams {
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
                        
        public static class Builder {
            
            private int numberOfQueries = 0;
            private double correlationWithGraph = 0;
            private double skew = 1;
            private int percentEdge = 50;
            private int percentVertex = 50;
            private int percentIntersection = 0;
         
            public Builder numberOfQueries(int nq){
                numberOfQueries = nq;
                return this;
            }
            
            public Builder skew(double sk){
                skew = sk;
                return this;
            }
            
            public Builder percentEdge(int pe){
                percentEdge = pe;
                return this;
            }
            
            public Builder percentVertex(int pv){
                percentVertex = pv;
                return this;
            }
            
            public Builder queryTypeDistribution(int percentEdge, int percentVertex, int percentIntersection){
                this.percentEdge = percentEdge;
                this.percentVertex = percentVertex;
                this.percentIntersection = percentIntersection;
                return this;
            }
            
            public WorkloadParams build(){
                //TODO: add other checks
                if (percentEdge + percentVertex + percentIntersection != 100 || skew <= 0 || 
                        correlationWithGraph > 1 || correlationWithGraph < 0 ||
                        numberOfQueries < 0){
                    throw new IllegalArgumentException();
                }
                
                WorkloadParams ans = new WorkloadParams();
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
}
