/**
 * 
 */
package com.twitter.dataservice.simulated;

public class ConstantSamplable implements Samplable {
	public enum Parameters {
		value;
	}
	
	  final int val;
	  public ConstantSamplable(int val){
		  this.val = val;
	  }
	  
	  public int sample(){
		  return val;
	  }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + val;
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
		ConstantSamplable other = (ConstantSamplable) obj;
		if (val != other.val)
			return false;
		return true;
	}  
  }