package com.twitter.dataservice.simulated;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;


import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Pair;
import com.twitter.dataservice.shardutils.TwoTierHashTest;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.parameters.GraphParameters;
import com.twitter.dataservice.parameters.WorkloadParameters;



public class SkewedDegreeGraphTest
{
    
    //use map counter instead
    private <K> void increaseCount(K key, Map<K, Integer> counts){
        if (!counts.containsKey(key)) counts.put(key, 0);
        
        counts.put(key, counts.get(key) + 1);
    }
      
    private void assertNumVertices(int expected, int maxDegree, SkewedDegreeGraph graph){
        Map<Vertex, Integer> vertices = new HashMap<Vertex, Integer>();
        Iterator<Edge> it = graph.graphIterator();
        
        while (it.hasNext()){
            Edge e = it.next();
            increaseCount(e.getLeftEndpoint(), vertices);
        }
        
        Assert.assertEquals(expected, vertices.size() + graph.getDisconnnectedVertices());
        for (Integer count: vertices.values()) Assert.assertTrue(count <= maxDegree);
    }
    
    @Test
    public void testAllVerticesPresent(){
        for (int i = 1; i < 10; ++i){
            for (int maxdeg = 1; maxdeg < 10; ++maxdeg){
                assertNumVertices(i, maxdeg*i, SkewedDegreeGraph.makeSkewedDegreeGraph(i, maxdeg*i, maxdeg, 1));
                assertNumVertices(i, maxdeg*i, SkewedDegreeGraph.makeSkewedDegreeGraph(i, maxdeg*i, maxdeg, 2));
            }
        }
    }
    
    @Test
    public void testConstructor(){
        SkewedDegreeGraph g1 = new SkewedDegreeGraph(new int[]{1,2,3});
        Assert.assertEquals(6, g1.getActualDegree());
        Assert.assertEquals(3, g1.getActualMaxDegree());
        Assert.assertEquals(0, g1.getDisconnnectedVertices());
        Assert.assertEquals(1, g1.getDegree(0));
        Assert.assertEquals(2, g1.getDegree(1));
        Assert.assertEquals(3, g1.getDegree(2));
        
        SkewedDegreeGraph g0; 
        
        try {
            g0 = new SkewedDegreeGraph(new int[]{1,0});
            Assert.fail();
        } catch (IllegalArgumentException e){}
        
        
        SkewedDegreeGraph g2 = new SkewedDegreeGraph(new int[]{1,2,1,1});
        
        Assert.assertEquals(5, g2.getActualDegree());
        Assert.assertEquals(2, g2.getActualMaxDegree());
        Assert.assertEquals(1, g2.getDegree(0));
        Assert.assertEquals(1, g2.getDegree(1));
        Assert.assertEquals(1, g2.getDegree(2));
        Assert.assertEquals(2, g2.getDegree(3));
        Assert.assertEquals(0, g2.getDisconnnectedVertices());
        
        Iterator<Edge> it = g2.graphIterator();
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals(new Edge(new Vertex(0), new Vertex(0)), it.next());
        //Assert.assertFalse(it.hasNext());
    }
    
    
    
    @Test
    public void testWorkloadIter(){
        SkewedDegreeGraph g = SkewedDegreeGraph.makeSkewedDegreeGraph(10, 100, 10, 1);
        
        List<TestingWorkNode> testNodes = new ArrayList<TestingWorkNode>(1);
        
        TestingWorkNode testNode = new TestingWorkNode();
        testNodes.add(testNode);
        
        APIServer testApi = APIServer.apiWithGivenWorkNodes(testNodes);
        for (int i = 0; i < 10; ++i){
            WorkloadParameters params = (new WorkloadParameters.Builder()).numberOfQueries(i).queryTypeDistribution(100, 0, 0).build();
            Iterator<Query> itq = g.workloadIterator(params);
            int count = 0;
            
            while (itq.hasNext()){   
                ++count;
                itq.next().execute(testApi);
            }
            
            Assert.assertEquals(i, count);
        }
    }
    
    @Test
    public void testWorkloadAndApi(){
        SkewedDegreeGraph g = SkewedDegreeGraph.makeSkewedDegreeGraph(10, 100, 10, 1);        
        List<TestingWorkNode> testNodes = new ArrayList<TestingWorkNode>(1);
        TestingWorkNode testNode = new TestingWorkNode();
        testNodes.add(testNode);
        
        APIServer testApi = APIServer.apiWithGivenWorkNodes(testNodes);
        
        int numq = 100;
        WorkloadParameters params = (new WorkloadParameters.Builder()).numberOfQueries(numq).queryTypeDistribution(100, 0, 0).build();
        Iterator<Query> itq = g.workloadIterator(params);

        while (itq.hasNext()){   
            itq.next().execute(testApi);
        }        

        Counter<Class> counts = testNode.getSummary();
        Assert.assertEquals(Integer.valueOf(numq), counts.getCount(Query.EdgeQuery.class));
    }
    
    /*
     * make sure numbering starts at 0
     */
    @Test
    public void testWorkloadGeneratesOne(){
        WorkloadParameters wp = new WorkloadParameters.Builder().numberOfQueries(10)
        .percentVertex(100).percentEdge(0).skew(0.01).build();  //low skew should not affect this, 
        
        Graph g = new SkewedDegreeGraph(new int[]{1});
        
        Iterator<Query> work = g.workloadIterator(wp);
        
        Query nx = work.next();
        Assert.assertTrue(work.next() instanceof Query.FanoutQuery);
        Assert.assertEquals(new Vertex(0), ((Query.FanoutQuery) nx).getVertex());
    }
    
    /*
     * checks setting ratio of 1 makes all vertices have the same degree.
     */
    @Test
    public void testConstantGraph(){
        
       for (int i = 1; i < 10; i++){
           GraphParameters gp = new GraphParameters.Builder()
               .degreeBoundAndTargetAvg(1, 1 << i)
               .numberVertices(100)
               .degreeSkew(1.0)
               .build();
           
           SkewedDegreeGraph g = SkewedDegreeGraph.makeSkewedDegreeGraph(gp);
           Iterator<Edge> git = g.graphIterator();
           MapBackedCounter<Vertex> counter = new MapBackedCounter<Vertex>();
           
           while (git.hasNext()) {
               counter.increaseCount(git.next().getLeftEndpoint());
           }
           
           TwoTierHashTest.assertBalanced((1 << i)*100, 100, 0.0, counter);
       }
    }
    
    @Test public void testFanoutIter(){
     	int numvert = 10;
    	int fanoutsz = 10;
    	GraphParameters gp = new GraphParameters.Builder()
    	.degreeBoundAndTargetAvg(1, fanoutsz)
    	.numberVertices(numvert)
    	.degreeSkew(1)
    	.build();
    	
    	SkewedDegreeGraph sk = SkewedDegreeGraph.makeSkewedDegreeGraph(gp);
    	Iterator<Pair<Integer, int[]>> it = sk.fanoutIterator();
    	for (int i = 0; i < numvert; i++){
    		Assert.assertTrue(it.hasNext());
    		Pair<Integer, int[]> curr = it.next();
    		Assert.assertEquals(Integer.valueOf(i), curr.getLeft());
    		Assert.assertEquals(fanoutsz, curr.getRight().length);
    	}
    	
    	Assert.assertTrue(!it.hasNext());
    }
        
}