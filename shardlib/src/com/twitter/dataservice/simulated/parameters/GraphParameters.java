/**
 * 
 */
package com.twitter.dataservice.simulated.parameters;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.twitter.dataservice.simulated.Graph;
import com.twitter.dataservice.simulated.SkewedDegreeGraph;

public class GraphParameters {
    private int numberVertices = 1;
    private int numberEdges = 1;
    private int maxDegree = 1;
    private double degreeSkewParameter = 1;
    
    //will add probably some other skew related parameters here
    public GraphParameters numberVertices(int num){
        this.numberVertices = num;
        return this;
    }
    
    public GraphParameters numberEdges(int edges){
        this.numberEdges = edges;
        return this;
    }

    public GraphParameters maxDegree(int max){
        this.maxDegree = max;
        return this;
    }
    
    public GraphParameters degreeSkew(double sk){
        this.degreeSkewParameter = sk;
        return this;
    }

    //TODO: control ordering
    public List<Map.Entry<String, Object>> fields(){
        Map<String,Object> temp = new HashMap<String,Object>();
        temp.put("numberVertices", numberVertices);
        temp.put("numberEdges", numberEdges);
        temp.put("maxDegree", maxDegree);
        temp.put("degreeSkew", degreeSkewParameter);
        
        return new LinkedList<Map.Entry<String, Object>>(temp.entrySet());
    }
    
    
    //TODO: fix size by edge, not vertex
    public Graph build(){
        assert numberVertices > 0;
        assert degreeSkewParameter > 1;
        assert this.maxDegree*this.numberVertices >= numberEdges;

        return new SkewedDegreeGraph(numberVertices, maxDegree, degreeSkewParameter);
    }              
}