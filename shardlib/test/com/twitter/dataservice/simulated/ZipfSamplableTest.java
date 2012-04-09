package com.twitter.dataservice.simulated;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import junit.framework.Assert;

import org.junit.Test;


public class ZipfSamplableTest {
	

	public void printProp(){
		System.out.println(ZipfSamplable.Parameters.exponent.toString());
	}
	
	@Test
	public void testZipfSamplerOffset(){
    	final int num = 10;
    	final int s = 1;
    	final int min = 1;
    	Samplable zipfy = new ZipfSamplable(min, num+min, s, num);
    	
    	MapBackedCounter<Integer> tally = new MapBackedCounter<Integer>();

    	int total = 100000; //seems to be pretty regular at 100k, not at 10k
    	for (int i = 0; i < total; i++){
    		tally.increaseCount(zipfy.sample());
    	}

    	int last = total;
    	int cumulative = 0;
    	Assert.assertTrue(tally.getCount(-1) == 0);
    	for (int i = min; i < num + min; i++){
    		Assert.assertTrue(String.format("i = %d, tally[i] = %d, last = %d", i, tally.getCount(i), last),
    				tally.getCount(i) <= last);
    		last = tally.getCount(i);
    		cumulative += last;
    	}
    	Assert.assertEquals(total, cumulative);
	}
	
	@Test 
	public void zipfSamplerLargeOffset(){
    	final int max = 115;
    	final float s = 1.0f;
    	final int min = 15;
    	final int resolution = 9;
    	//so expect min to be 15 and max val seen to be 114 or maybe a bit less
    	Samplable zipfy = new ZipfSamplable(min, max, s, resolution);
    	
    	MapBackedCounter<Integer> tally = new MapBackedCounter<Integer>();

    	int total = 100000; //seems to be pretty regular at 100k, not at 10k
    	for (int i = 0; i < total; i++){
    		tally.increaseCount(zipfy.sample());
    	}

    	int last = total;
    	int cumulative = 0;
    	Assert.assertTrue(tally.getCount(-1) == 0);
    	for (int i = min; i < max; i++){
    		Assert.assertTrue(String.format("i = %d, tally[i] = %d, last = %d", i, tally.getCount(i), last),
    				tally.getCount(i) <= last*1.2); // some tolerance for constant intervals
    		last = tally.getCount(i);
    		cumulative += last;
    	}
    	Assert.assertEquals(total, cumulative);
	}
	
    @Test
    public void testZipfSampler(){
    	final int num = 10;
    	final int s = 1;
    	Samplable zipfy = new ZipfSamplable(0, num, s, num);
    	
    	MapBackedCounter<Integer> tally = new MapBackedCounter<Integer>();

    	int total = 100000; //seems to be pretty regular at 100k, not at 10k
    	for (int i = 0; i < total; i++){
    		tally.increaseCount(zipfy.sample());
    	}

    	int last = total;
    	int cumulative = 0;
    	Assert.assertTrue(tally.getCount(-1) == 0);
    	for (int i = 0; i < num; i++){
    		Assert.assertTrue(String.format("i = %d, tally[i] = %d, last = %d", i, tally.getCount(i), last),
    				tally.getCount(i) <= last);
    		last = tally.getCount(i);
    		cumulative += last;
    	}
    	//Assert.assertTrue(tally.getCount(num) == 0);    	
    	Assert.assertEquals(total, cumulative);
    }

    //write samples to a file for later plotting (to check generators look good)
    public void generateZipfSample(){
    	long start = System.nanoTime();
    	int num = 10000000;
    	int bins = 10;
    	float s = 2.0f;
    	Samplable zipfy = new ZipfSamplable(0, 1000, s, bins);	
    	File f = new File(String.format("/home/orm/thesis/shardlib/test.tmp", num, s));
    	
    	try {
			PrintStream bos = new PrintStream(new FileOutputStream(f));
	    	for (int i = 0; i < 100000; i++){
	    		bos.println(zipfy.sample());
	    	}
    	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
    	
    	System.out.println((System.nanoTime() - start)/1000);
    }

}
