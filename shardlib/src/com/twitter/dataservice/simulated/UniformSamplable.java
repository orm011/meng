package com.twitter.dataservice.simulated;

import java.util.Random;

public class UniformSamplable implements Samplable {
	
		public enum Parameters {
			lower,
			upper
		}
	  final Random rand;
	  final int low_limit;
	  final int high_limit;
	  
	  public UniformSamplable(Random rand, int limit){
		  this(rand, 0, limit);
	  }
	  
	  public UniformSamplable(Random rand, int lower, int upper){
		  if (!(lower <  upper)) throw new IllegalArgumentException();
		  
		  this.rand = rand;
		  this.low_limit = lower;
		  this.high_limit = upper;
	  }
	  
	  public UniformSamplable(int lower, int upper) {
		  this(new Random(), lower, upper);
	  }

	public int sample(){
		  return low_limit + rand.nextInt(high_limit - low_limit);
	  }

	@Override
	public String toString() {
		return "UniformSamplable [high_limit=" + high_limit + ", low_limit="
				+ low_limit + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + high_limit;
		result = prime * result + low_limit;
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
		UniformSamplable other = (UniformSamplable) obj;
		if (high_limit != other.high_limit)
			return false;
		if (low_limit != other.low_limit)
			return false;
		return true;
	}
}
