package com.twitter.dataservice.simulated;

import java.util.Random;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.AbstractIntegerDistribution;
import org.apache.commons.math.distribution.ZipfDistributionImpl;

/*
 * needs to return a number between 0 and some upper limit (id space starts at 0)
 */
public class ZipfSamplable implements Samplable {

	public enum Parameters {
		lower,
		upper,
		exponent,
		resolution
	}
	  final AbstractIntegerDistribution aid;
	  final Random rand = new Random();
	  final int lower;
	  final int upper;
	  final float exponent;
	  final int resolution; //numbins
	  
	  /*
	   * @param(lower) is the lowest pos value (default 0)
	   * @param(upper) 
	   * @param(exponent) the zipf exponent. the larger, the more the expected val is closer to lower.
	   * @param(resolution) the number of different vals allowed (note, if you want 
	   * 99.9 percentile to mean more than 99 percentile, you need to sample at least 1000.
	   * and you need a resolution of more than 10000k probably?
	   *TODO: figure out if I can improve generator performance by doing
	   *a multilevel zipf.ie, in order to do 10k do 100 to pick the bin then 100 again to pick the index. 
	   */
	  public ZipfSamplable(int lower, int upper, float exponent, int resolution){
		  this.aid = new ZipfDistributionImpl(resolution, exponent);
		  this.lower = lower;
		  this.upper = upper;
		  this.exponent = exponent;
		  this.resolution = resolution;
	  }
	  
	  public int sample() {
		  int range = upper - lower;
		  int binsize = range/resolution;
		  try {
			return (aid.sample() - 1)*binsize + rand.nextInt(binsize) + lower;
		} catch (MathException e) {
			throw new RuntimeException(e);
		}
	  }

	@Override
	public String toString() {
		return "ZipfSamplable [exponent=" + exponent + ", lower=" + lower
				+ ", resolution=" + resolution + ", upper=" + upper + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Float.floatToIntBits(exponent);
		result = prime * result + lower;
		result = prime * result + resolution;
		result = prime * result + upper;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ZipfSamplable other = (ZipfSamplable) obj;
		if (Float.floatToIntBits(exponent) != Float
				.floatToIntBits(other.exponent))
			return false;
		if (lower != other.lower)
			return false;
		if (resolution != other.resolution)
			return false;
		if (upper != other.upper)
			return false;
		return true;
	}	  
}