package com.twitter.dataservice.simulated;

import java.util.Iterator;
import java.util.List;

//class with test queries to check client server communication
public class TestWorkloadIterator implements Iterator<Query> {

	
	private List<Query> queries;
	public TestWorkloadIterator(){
	}
	
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Query next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}

}
