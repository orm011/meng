/**
 * 
 */
package com.twitter.dataservice.simulated.parameters;

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
    private int numberVertices;
    private int numberEdges;
    private int upperDegreeBound;
    private int average;
    private double degreeSkewParameter;

    
    public int getNumberEdges(){
        //TODO: assert build() has been called or get rid
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
    //will add probably some other skew related parameters here
    public GraphParameters numberVertices(int num){
        this.numberVertices = num;
        return this;
    }
    
    public GraphParameters degreeBoundAndTargetAvg(int max, int average){
        this.upperDegreeBound = max;
        this.average = average;
        return this;
    }
    
    public GraphParameters degreeSkew(double sk){
        this.degreeSkewParameter = sk;
        return this;
    }

    //TODO: control ordering
    public List<Map.Entry<String, Object>> fields(){
        //TODO: check if built yet. 
        Map<String,Object> temp = new LinkedHashMap<String,Object>();
        temp.put("numberVertices", numberVertices);
        temp.put("numberEdges", numberEdges);
        temp.put("upperBoundOnDegree", upperDegreeBound); //note this is no longer valid, TODO: remove
        temp.put("degreeSkew", degreeSkewParameter);

        return new LinkedList<Map.Entry<String, Object>>(temp.entrySet());
    }
    
    
    public Graph build() {
        boolean check = 
            numberVertices > 0  
            && degreeSkewParameter > 0;
        //    && numberVertices > upperDegreeBound;
            
        if (!check) throw new IllegalArgumentException("graph params invalid");
        
        this.numberEdges = numberVertices*average;
        SkewedDegreeGraph g = SkewedDegreeGraph.makeSkewedDegreeGraph(numberVertices, numberEdges, upperDegreeBound, degreeSkewParameter);
        return g;
    }
}