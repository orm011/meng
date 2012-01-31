/**
 * 
 */
package com.twitter.dataservice.simulated;

import java.util.Collections;
import java.util.List;

import com.twitter.dataservice.shardutils.Vertex;

public abstract class Query {
    public static class EdgeQuery extends Query{
        private Vertex right;
        private Vertex left;
        
        public Vertex getLeftVertex(){
            return left;
        }
        
        public Vertex getRightVertex(){
            return right;
        }
        
        EdgeQuery(Vertex v, Vertex w){
            left = v;
            right = w;
        }
        
        @Override
        public List<Vertex> execute(IAPIServer api){
            api.getEdge(left, right);
            return Collections.emptyList();
        }
        
        @Override
        public String toString(){
            return String.format("EdgeQ: %s %s", left, right);
        }
    }
    
    public static class FanoutQuery extends Query{
        private Vertex v;
        private int pageSize;
        private int offset;
        
        public Vertex getVertex(){
            return v;
        }
        
        public int getPageSize(){
            return this.pageSize;
        }
        
        public int getOffset(){
            return this.offset;
        }
        
        public FanoutQuery(Vertex v, int pageSize, int offset){
            this.v = v;
            this.pageSize = pageSize;
            this.offset = offset;
        }
        
        //TODO: pass full set of arguments from workload.
        private FanoutQuery(Vertex v){
            this(v, Integer.MAX_VALUE, -1);
        }
        
        @Override
        public List<Vertex> execute(IAPIServer api){
             return api.getFanout(v, pageSize, offset);
        }
        
        //TODO: use the one coming from the log
        @Override
        public String toString(){
            return String.format("FanoutQ: %s, %d, %d", v, pageSize, offset);
        }
    }
    
    public static class IntersectionQuery extends Query {
        private Vertex left;
        private Vertex right;
        private int pageSize;
        private int offset;
        
        public Vertex getLeftVertex(){
            return left;
        }
        
        public Vertex getRightVertex(){
            return right;
        }
        public int getPageSize(){
            return this.pageSize;
        }
        
        public int getOffset(){
            return this.offset;
        }
        
        public IntersectionQuery(Vertex v, Vertex w, int pageSize, int offset){
            left = v;
            right = w;
            this.pageSize = pageSize;
            this.offset = offset;
        }
        
        private IntersectionQuery(Vertex v, Vertex w){
            this(v, w, Integer.MAX_VALUE, -1);
        }
        
        @Override
        public List<Vertex> execute(IAPIServer api){
            return api.getIntersection(this.left, this.right, pageSize, offset);
        }
        
        @Override
        public String toString(){
            return String.format("IntersectionQ %s %s %d %d", left, right, pageSize, offset);
        }
    }
    
    /*
     * used for unsuported queries
     */
    public static class NopQuery extends Query {
        
        String line;
        public NopQuery(String line){
            this.line = line;
        }
        
        @Override
        public List<Vertex> execute(IAPIServer api){
            return Collections.EMPTY_LIST;
        }
        
        public String toString(){
            return String.format("NopQuery: %s", this.line);
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
    
    public abstract List<Vertex> execute(IAPIServer api);
}