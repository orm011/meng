package com.twitter.dataservice.simulated;

import java.util.Properties;

import junit.framework.Assert;

import org.junit.Test;

public class BenchmarkTest {
	
	@Test
	public void readSamplerTest(){
		Properties prop = Benchmark.parsePropertyFile("test/com/twitter/dataservice/simulated/testConfigSamplable.properties");

		Samplable s = Benchmark.getSamplable(prop, "workload");		
		Assert.assertEquals(new ZipfSamplable(1, 11, 1.1f, 10), s);

		Samplable t = Benchmark.getSamplable(prop, "bar");
		Assert.assertEquals(new UniformSamplable(2, 12), t);

		Samplable u = Benchmark.getSamplable(prop, "baz");
		Assert.assertEquals(new ConstantSamplable(10), u);
		
		try {
		Samplable v = Benchmark.getSamplable(prop, "foo");
		Assert.fail();
		} catch (IllegalArgumentException e){}
		
		
		Samplable w = Benchmark.getSamplable(prop, "moo");
		Assert.assertFalse(w.equals(s));
	}
	

}
