package com.twitter.dataservice.simulated;

import static org.junit.Assert.*;

import java.rmi.RemoteException;

import org.junit.Test;

import com.twitter.dataservice.shardutils.Vertex;

public class BriefDataNodeTest {

	@Test
	public void testGetFanoutVertexIntInt() {
		BriefDataNode bdn = new BriefDataNode(10, "node1", 2);
		bdn.putFanout(0, new int[]{0,1,2,3,4,5});
		bdn.putFanout(2, new int[]{});
		
		try {
			int[] ans = bdn.getFanout(new Vertex(0), 10, 0);
			assertTrue(ans.length == 2);
			assertTrue(ans[0] == 30);
			assertTrue(ans[1] == 0);
			
			int[] ans3 = bdn.getFanout(new Vertex(2), 10, 0);
			assertTrue(ans3.length == 1);
			assertTrue(ans3[0] == 0);
			
			//TODO should make Data node throw NoSuchElt. Exception whenever element is missing.
			try {
				bdn.getFanout(new Vertex(1), 10, 0);
				fail("should throw exception");
			} catch (RemoteException e){}
		} catch (RemoteException e) {
			fail("should not have remote exceptions");
		}
	}

}
