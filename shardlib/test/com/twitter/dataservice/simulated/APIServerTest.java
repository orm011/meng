package com.twitter.dataservice.simulated;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import org.junit.Assert;
import org.junit.Test;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.remotes.IDataNode.NodeStats;
import com.twitter.dataservice.shardingpolicy.VertexHashSharding;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.simulated.IAPIServer.Stats;
import com.twitter.dataservice.simulated.IAPIServer.SuccessfulStats;

public class APIServerTest {
	
	public Map<Node, IDataNode> makeCompactNodes(int numNodes){
		HashMap<Node, IDataNode> ans = new HashMap<Node, IDataNode>(numNodes);
		
		for (int i = 0; i < numNodes; i++){
			ans.put(new Node(i), new CompactDataNode(0, "node" + Integer.toString(i)));
		}
		
		return ans;
	}
	
	/*
	 * tests both the individual stats and the aggregate stats
	 */
	@Test
	public void testStat(){	
		APIServer api = new APIServer(makeCompactNodes(2), new VertexHashSharding(2));
		Collection<IAPIServer.Stats> stats = api.stat();
		
		
		IDataNode.NodeStats node0 = new NodeStats("node0", 0, 0, -1), node1 = new NodeStats("node1", 0, 0, -1);
		
		Iterator<IAPIServer.Stats> iter = stats.iterator();
		IAPIServer.SuccessfulStats curr;
		Assert.assertEquals(node0, (curr = (SuccessfulStats)iter.next()).nstats);
		Assert.assertTrue(curr.time > 0);
		
		Assert.assertEquals(node1, (curr = (SuccessfulStats)iter.next()).nstats);
		Assert.assertTrue(curr.time > 0);
		
		Assert.assertTrue(!iter.hasNext());
		
		SuccessfulStats summary = APIServer.statSummary(stats);
		Assert.assertEquals(new NodeStats(":node0:node1", 0, 0, -1), summary.nstats);
		
		api.putFanout(0, new int[]{1,2});
		Assert.assertEquals(new NodeStats(":node0:node1", 1, 2, 2), APIServer.statSummary(api.stat()).nstats);
		
		api.putFanout(1, new int[]{0,1,2,3,4});
		Assert.assertEquals(new NodeStats(":node0:node1", 2, 7, 5), APIServer.statSummary(api.stat()).nstats);
	}

}
