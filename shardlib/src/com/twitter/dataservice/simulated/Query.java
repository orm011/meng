/**
 * 
 */
package com.twitter.dataservice.simulated;

import com.twitter.dataservice.shardutils.Vertex;

public abstract class Query {
    static class EdgeQuery extends Query{
        Vertex left;
        Vertex right;
        
        private EdgeQuery(Vertex v, Vertex w){
            left = v;
            right = w;
        }
        
        @Override
        public void execute(IAPIServer api){
            api.getEdge(left, right);
        }
    }
    
    static class FanoutQuery extends Query{
        Vertex v;
        
        private FanoutQuery(Vertex v){
            this.v = v;
        }
        
        @Override
        public void execute(IAPIServer api){
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
        public void execute(IAPIServer api){
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
    
    public abstract void execute(IAPIServer api);
}