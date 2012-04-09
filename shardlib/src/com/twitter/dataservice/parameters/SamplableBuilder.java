package com.twitter.dataservice.parameters;

import java.util.Random;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.primitives.Ints;
import com.twitter.dataservice.simulated.ConstantSamplable;
import com.twitter.dataservice.simulated.Samplable;
import com.twitter.dataservice.simulated.UniformSamplable;
import com.twitter.dataservice.simulated.ZipfSamplable;

public class SamplableBuilder {
	
	public static enum DistributionType {
		ZIPF,
		UNIFORM,
		CONSTANT
	}

	DistributionType type = null;
	private int zipfbins = 0;
	private int lowLimit = 1;
	private int highLimit = -1;
	private float skew = 0;
	private int value = 0;
	private boolean valuDefined = false;
	/**
	 * @param t (type)
	 * @param lowLimit (uniform, zipf)
	 * @param highLimit (uniform, zipf)
	 * @param skew (zipf)
	 * @param ins (for zipf)
	 */
	
	public Samplable build(){
		if (type == null) throw new IllegalArgumentException("need type");

		Samplable ans = null;
		if (type.equals(DistributionType.UNIFORM)){
			if (!(lowLimit >= 0)) throw new IllegalArgumentException();
			if (!(highLimit  > lowLimit)) throw new IllegalArgumentException();
			if (valuDefined) throw new IllegalArgumentException();

			ans = new UniformSamplable(lowLimit, highLimit);
		} else if (type.equals(DistributionType.ZIPF)){
			if (!(skew > 0)) throw new IllegalArgumentException();
			if (!(zipfbins > 0)) throw new IllegalArgumentException();	
			if (!(lowLimit >= 0)) throw new IllegalArgumentException();
			if (!(highLimit > lowLimit)) throw new IllegalArgumentException();
			if (valuDefined) throw new IllegalArgumentException();

			zipfbins = Ints.min(zipfbins, highLimit - lowLimit);
			ans = new ZipfSamplable(lowLimit, highLimit, skew, zipfbins);
		} else if (type.equals(DistributionType.CONSTANT)){
			if (!valuDefined) throw new IllegalArgumentException();
			ans =  new ConstantSamplable(value);
		} else {
			throw new NotImplementedException("unknown distribution type");
		}
		
		return ans;
	}
	
	public int getZipfbins() {
		return zipfbins;
	}

	public void setZipfbins(int zipfbins) {
		this.zipfbins = zipfbins;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
		this.valuDefined = true;
	}

	public DistributionType getType() {
		return type;
	}
	
	public void setType(DistributionType type) {
		this.type = type;
	}

	public int getLowLimit() {
		return lowLimit;
	}

	public void setLowLimit(int lowLimit) {
		this.lowLimit = lowLimit;
	}

	public int getHighLimit() {
		return highLimit;
	}

	public void setHighLimit(int highLimit) {
		this.highLimit = highLimit;
	}

	public float getSkew() {
		return skew;
	}

	public void setSkew(float skew) {
		this.skew = skew;
	}	
}
