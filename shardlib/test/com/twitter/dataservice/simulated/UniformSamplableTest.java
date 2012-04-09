package com.twitter.dataservice.simulated;

import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import com.twitter.dataservice.shardutils.TwoTierHashTest;


public class UniformSamplableTest {

    
    @Test
    public void testUniformSampler(){
    	for (int numbins = 1; numbins < 100; numbins += 10){
    		
    		int min = 10; int max = 10 + numbins;
    		Samplable unis = new UniformSamplable(new Random(), 0, numbins);
    		Samplable unis_off = new UniformSamplable(new Random(), min, max);
    		MapBackedCounter<Integer> tally = new MapBackedCounter<Integer>();
    		MapBackedCounter<Integer> tally_off = new MapBackedCounter<Integer>();

	    	int numparticles = numbins*10000;
	    	for (int i = 0; i < numparticles; i++){
	    		tally.increaseCount(unis.sample());
	    		tally_off.increaseCount(unis_off.sample());
	    	}
	    	
	    	//int numParticles, int numBins, double maxSpreadTolerated, MapBackedCounter<T> tally
	    	TwoTierHashTest.assertBalanced(numparticles, numbins, 0.2, tally);
	    	TwoTierHashTest.assertBalanced(numparticles, numbins, 0.2, tally_off);

	    	Assert.assertTrue(tally.getCount(0) > 0);
	    	Assert.assertTrue(tally.getCount(numbins - 1) > 0);
	    	Assert.assertTrue(tally.getCount(numbins) == 0);
	    	Assert.assertTrue(tally.getCount(-1) == 0);

	    	Assert.assertTrue(tally_off.getCount(min) > 0);
	    	Assert.assertTrue(tally_off.getCount(max - 1) > 0);
	    	Assert.assertTrue(tally_off.getCount(max) == 0);
	    	Assert.assertTrue(tally_off.getCount(min - 1) == 0);
	    	
	    	int total = 0;
	    	for (int i = min; i < max; i++){
	    		total += tally_off.getCount(i);
	    	}
	    	Assert.assertEquals(numparticles, total);
	    	
    	}
    }	
}
