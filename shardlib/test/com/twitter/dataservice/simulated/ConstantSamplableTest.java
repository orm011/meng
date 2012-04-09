package com.twitter.dataservice.simulated;

import junit.framework.Assert;

import org.junit.Test;


public class ConstantSamplableTest {

    @Test
    public void testConstantSample(){
    	
    	for (int value = 0; value < 5; value++){
	    	Samplable constSampler = new ConstantSamplable(value);
	    	for (int i = 0; i < 10; i++){
	    		Assert.assertEquals(value, constSampler.sample());
	    	}
    	}
    }
	
}
