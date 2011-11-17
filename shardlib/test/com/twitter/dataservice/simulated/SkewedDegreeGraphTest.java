package com.twitter.dataservice.simulated;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.plaf.multi.MultiSeparatorUI;

import junit.framework.Assert;

import org.junit.Test;

import com.twitter.dataservice.remotes.ICompleteWorkNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.parameters.WorkloadParams;



public class SkewedDegreeGraphTest
{
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
        
        Assert.assertEquals(expected, vertices.size());
        for (Integer count: vertices.values()) Assert.assertTrue(count <= maxDegree);
    }
    
    @Test
    public void testAllVerticesPresent(){
        for (int i = 1; i < 10; ++i){
            for (int maxdeg = 1; maxdeg < 10; ++maxdeg){
                assertNumVertices(i, maxdeg, new SkewedDegreeGraph(i, maxdeg, 1));
                assertNumVertices(i, maxdeg, new SkewedDegreeGraph(i, maxdeg, 2));
            }
        }
    }
    
    @Test
    public void testWorkloadIter(){
        SkewedDegreeGraph g = new SkewedDegreeGraph(10, 10, 1);
        
        List<TestingWorkNode> testNodes = new ArrayList<TestingWorkNode>(1);
        
        TestingWorkNode testNode = new TestingWorkNode();
        testNodes.add(testNode);
        
        APIServer testApi = APIServer.apiWithGivenWorkNodes(testNodes);
        for (int i = 0; i < 10; ++i){
            WorkloadParams params = (new WorkloadParams.Builder()).numberOfQueries(i).queryTypeDistribution(100, 0, 0).build();
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
        SkewedDegreeGraph g = new SkewedDegreeGraph(10, 10, 1);        
        List<TestingWorkNode> testNodes = new ArrayList<TestingWorkNode>(1);
        TestingWorkNode testNode = new TestingWorkNode();
        testNodes.add(testNode);
        
        APIServer testApi = APIServer.apiWithGivenWorkNodes(testNodes);
        
        int numq = 100;
        WorkloadParams params = (new WorkloadParams.Builder()).numberOfQueries(numq).queryTypeDistribution(100, 0, 0).build();
        Iterator<Query> itq = g.workloadIterator(params);

        while (itq.hasNext()){   
            itq.next().execute(testApi);
        }        

        Counter<Class> counts = testNode.getSummary();
        Assert.assertEquals(Integer.valueOf(numq), counts.getCount(Query.EdgeQuery.class));
    }
}
