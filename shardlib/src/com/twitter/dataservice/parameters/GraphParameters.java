/**
 * 
 */
package com.twitter.dataservice.parameters;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.twitter.dataservice.simulated.Samplable;


public class GraphParameters extends AbstractParameters {
    //prescribed values. Note maxdegree is usually much less than the bound.
    //numberEdges is a function of our target average  and num Vertices.
        
    //NOTE: don't change this without changing existing config files as well.
	public static final String prefix = "graph";
	public static final String NUMBER_VERTICES = "graph.numberVertices";
    public static final String DISTRIBUTION_TYPE = "graph.distributionType";
    	
    private final int numberVertices;
    private final Samplable rand;
    
     
    public int getNumberVertices(){
        return this.numberVertices;
    }
    
    public Samplable getSamplable(){
    	return this.rand;
    }
    
    public GraphParameters(int numberVertices, Samplable rand){
    	super();
    	assert rand != null;
    	assert numberVertices > 0;
    	
    	this.numberVertices = numberVertices;
    	this.rand = rand;
    }

    //kept here to now break the test build
    public GraphParameters(int numberVertices, int numberEdges, int upperDegreeBound, int average,
            double degreeSkewParameter)
    {
        super();
        this.numberVertices = numberVertices;
        
        SamplableBuilder b = new SamplableBuilder();
        b.setHighLimit(upperDegreeBound);
        b.setLowLimit(0);
        b.setSkew((float)degreeSkewParameter);
        b.setZipfbins(upperDegreeBound);
        
        this.rand = b.build();
    }
 
    public List<Map.Entry<String, Object>> fields(){
        Map<String,Object> temp = new LinkedHashMap<String,Object>();
        temp.put(NUMBER_VERTICES, numberVertices);
        
        return new LinkedList<Map.Entry<String, Object>>(temp.entrySet());
    }
    
    public static class Builder {
        int numberVertices;
        Samplable rand;

    public Builder numberVertices(int num){
        this.numberVertices = num;
        return this;
    }
    
    public Builder degreeGenerator(Samplable rand){
    	this.rand = rand;
    	return this;
    }

    public GraphParameters build(){
        
        boolean check = numberVertices > 0;
        if (!check) throw new IllegalArgumentException("graph params invalid");
        return new GraphParameters(this.numberVertices, this.rand);
    }
    }
}