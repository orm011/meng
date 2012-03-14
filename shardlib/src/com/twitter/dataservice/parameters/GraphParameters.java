/**
 * 
 */
package com.twitter.dataservice.parameters;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.twitter.dataservice.simulated.Graph;
import com.twitter.dataservice.simulated.SkewedDegreeGraph;

public class GraphParameters extends AbstractParameters {
    //prescribed values. Note maxdegree is usually much less than the bound.
    //numberEdges is a function of our target average  and num Vertices.
        
    //NOTE: don't change this without changing existing config files as well.
    public static final String DEGREE_RATIO_BOUND = "graph.degreeRatioBound";  
    public static final String AVERAGE_DEGREE = "graph.averageDegree";
    public static final String SKEW_PARAMETER = "graph.degreeSkew";
    public static final String NUMBER_VERTICES = "graph.numberVertices";
    
    private final int numberVertices;
    private final int numberEdges;
    private final int upperDegreeBound;
    private final int average;
    private final double degreeSkewParameter;

    
    public int getNumberEdges(){
        return this.numberEdges;
    }
    
    public int getUpperDegreeBound(){
        return this.upperDegreeBound;
    }
    
    public int getTargetAverage(){
        return this.average;
    }
    
    public int getNumberVertices(){
        return this.numberVertices;
    }
    
    public double getDegreeSkewParameter(){
        return this.degreeSkewParameter;
    }
    
    
    public GraphParameters(int numberVertices, int numberEdges, int upperDegreeBound, int average,
            double degreeSkewParameter)
    {
        super();
        this.numberVertices = numberVertices;
        this.numberEdges = numberEdges;
        this.upperDegreeBound = upperDegreeBound;
        this.average = average;
        this.degreeSkewParameter = degreeSkewParameter;
    }
 
    public List<Map.Entry<String, Object>> fields(){
        Map<String,Object> temp = new LinkedHashMap<String,Object>();
        temp.put(NUMBER_VERTICES, numberVertices);
        temp.put(AVERAGE_DEGREE, average);
        temp.put(DEGREE_RATIO_BOUND, upperDegreeBound);
        temp.put(SKEW_PARAMETER, degreeSkewParameter);
        
        return new LinkedList<Map.Entry<String, Object>>(temp.entrySet());
    }
    
    public static class Builder {
        int numberVertices;
        int degreeRatioBound;
        int average;
        double degreeSkewParameter;

    //will add probably some other skew related parameters here
    public Builder numberVertices(int num){
        this.numberVertices = num;
        return this;
    }
    
    public Builder degreeBoundAndTargetAvg(int maxOverMinRatio, int targetAverage){
        this.degreeRatioBound = maxOverMinRatio;
        this.average = targetAverage;
        return this;
    }
    
    public Builder degreeSkew(double sk){
    	assert sk > 0; // should this be > 1?
        this.degreeSkewParameter = sk;
        return this;
    }

    public GraphParameters build(){
        
        boolean check = 
            numberVertices > 0  
            && degreeSkewParameter > 0;
            
        if (!check) throw new IllegalArgumentException("graph params invalid");
        
        return new GraphParameters(this.numberVertices, this.numberVertices*this.average, this.degreeRatioBound, this.average, this.degreeSkewParameter);
        }
    }
}