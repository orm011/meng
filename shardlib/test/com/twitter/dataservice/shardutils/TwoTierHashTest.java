package com.twitter.dataservice.shardutils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.twitter.dataservice.parameters.GraphParameters;
import com.twitter.dataservice.parameters.SamplableBuilder;
import com.twitter.dataservice.parameters.SamplableBuilder.DistributionType;
import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.sharding.CycleIterator;
import com.twitter.dataservice.sharding.ISharding;
import com.twitter.dataservice.sharding.SlidingWindowCycleIterator;
import com.twitter.dataservice.shardingpolicy.TwoTierHashSharding;
import com.twitter.dataservice.simulated.Counter;
import com.twitter.dataservice.simulated.Graph;
import com.twitter.dataservice.simulated.MapBackedCounter;
import com.twitter.dataservice.simulated.SkewedDegreeGraph;
import com.twitter.dataservice.simulated.SkewedDegreeGraphTest;

public class TwoTierHashTest
{
    List<Pair<Token, Node>> state;
    TwoTierHashSharding tths = null;
    SortedSet<Token> tokenset = null;
    
    byte[][] bytez = {
            new byte[]{(byte) 0x00, (byte) 0xff},
            new byte[]{(byte) 0x0f, (byte) 0xff},
            new byte[]{(byte) 0xef, (byte) 0xff},
    };
    
    final Token TOKENx00 = new Token(new byte[]{(byte) 0x00});
    final Token TOKENx0F = new Token(new byte[]{(byte) 0x0f});
//    
    final Token TOKENx10 = new Token(new byte[]{(byte) 0x10});
    final Token TOKENx08 = new Token(new byte[]{(byte) 0x08});
    final Token TOKENx18 = new Token(new byte[]{(byte) 0x18});        
    final Token TOKENx01 = new Token(new byte[]{(byte) 0x01});
    final Token TOKENx11 = new Token(new byte[]{(byte) 0x11});
    final Token TOKENxFF = new Token(new byte[]{(byte) 0xff});
    final Token TOKENx0B = new Token(new byte[]{(byte) 0x0b});

    Token[] testShardSet = new Token[]{
            TOKENx10,
            TOKENx08,
            TOKENx18
    };
    
    Token[] testTokens = new Token[]{
            TOKENx01,
            TOKENx10,
            TOKENx11,
            TOKENxFF
    };
    
    @Before
    public void prepareShardState(){
        state = new LinkedList<Pair<Token, Node>>();
        
        for (int  i = 0; i < bytez.length; i++){
            Pair<Token, Node> first = new Pair<Token, Node>(new Token(bytez[i]), new Node(i));
            state.add(first);
        }
        
        tths = new TwoTierHashSharding(state);    

        tokenset = new TreeSet<Token>();
        for (Pair<Token, Node> p: state)
            tokenset.add(p.getLeft());
        
    }
    
    
    
    @Test
    public void cycleIteratorTest(){
        
        //try a single element set
        SortedSet<Token> sizeOneSet = new TreeSet<Token>(Arrays.asList(new Token[]{TOKENx0F}));
        Iterator<Token> szOneIter = sizeOneSet.iterator();
        szOneIter.next();
        CycleIterator<Token> single = new CycleIterator<Token>(sizeOneSet, szOneIter);
        Assert.assertTrue(sizeOneSet.size() == 1);
        
        Assert.assertTrue(!single.wrappedAround());
        for (int i = 0; i < 5; i++){
            Assert.assertTrue(single.hasNext());
            Assert.assertTrue(single.next().equals(TOKENx0F));
            
            Assert.assertTrue(single.wrappedAround());
        }
        
        //several elements
        //initialize iterator to offset
        Token[] twoTokens = new Token[]{TOKENx0F, TOKENx18};
        SortedSet<Token> sizeTwoSet = new TreeSet<Token>(Arrays.asList(twoTokens));
        Iterator<Token> usedIter = sizeTwoSet.iterator();
        usedIter.next();
        
        CycleIterator<Token> at = new CycleIterator<Token>(sizeTwoSet, sizeTwoSet.iterator());
        CycleIterator<Token> et = new CycleIterator<Token>(sizeTwoSet);
        CycleIterator<Token> it = new CycleIterator<Token>(sizeTwoSet, usedIter);
        Assert.assertTrue(at.hasNext() && et.hasNext() && it.hasNext());

        Assert.assertTrue(sizeTwoSet.size() == 2);
        //iterate three times check it wraps around contents, 
        //check the wrapped flag is set at the right times only.
        for (int i = 0; i < 3*bytez.length; i++){
            Assert.assertTrue(at.next().equals(twoTokens[i % 2]));
            Assert.assertTrue(et.next().equals(twoTokens[i % 2]));
            Assert.assertTrue(at.wrappedAround() == ((i % 2) == 0 && i != 0));
            Assert.assertTrue(at.wrappedAround() == et.wrappedAround());
            Assert.assertTrue(it.next().equals(twoTokens[(i + 1) % 2]));
            Assert.assertTrue(it.wrappedAround() == (((i + 1) % 2) == 0));
        }

    }
    
    @Test
    public void windowIteratorTest(){
        CycleIterator<Token> cycle1 = new CycleIterator<Token>(tokenset);
        CycleIterator<Token> cycle2 = new CycleIterator<Token>(tokenset);
        CycleIterator<Token> cycle3 = new CycleIterator<Token>(tokenset);
        
        Iterator<List<Token>> windowcycle1 = new SlidingWindowCycleIterator<Token>(cycle1, 1);       
        Iterator<List<Token>> windowcycle2 = new SlidingWindowCycleIterator<Token>(cycle2, 2); 
        Iterator<List<Token>> windowcycle4 = new SlidingWindowCycleIterator<Token>(cycle3, 4);
        
        Assert.assertTrue(windowcycle1.hasNext() && windowcycle2.hasNext() && windowcycle4.hasNext());

        List<Token> next1;
        List<Token> next2;
        List<Token> next4;
 
        for (int i = 0; i < 3*bytez.length; i++){
            next1 = windowcycle1.next();
            Assert.assertEquals(1, next1.size());

            next2 = windowcycle2.next();
            Assert.assertEquals(2, next2.size());
            
            next4 = windowcycle4.next();
            Assert.assertEquals(4, next4.size());
            
            //test the results lists have the correct values, including ordering            
            Assert.assertEquals(new Token(bytez[i % bytez.length]), next1.get(0));
            Assert.assertEquals(next2.get(0), next1.get(0));
            Assert.assertEquals(new Token(bytez[(i+1) % bytez.length]), next2.get(1));
            Assert.assertEquals(next2.get(0), next4.get(0));
            Assert.assertEquals(next4.get(1), next2.get(1));
            Assert.assertEquals(new Token(bytez[(i+2) % bytez.length]), next4.get(2));
            Assert.assertEquals(new Token(bytez[(i+3) % bytez.length]), next4.get(3));
        }
        
    }
    
    private TwoTierHashSharding makeSharding(List<Token> tokens){
        int i = 0;
        
        List<Pair<Token, Node>> in = new LinkedList<Pair<Token,Node>>();
        for (Token tk: tokens){
            Pair<Token, Node> pr = new Pair<Token, Node>(tk, new Node(i));            
            i++;
            in.add(pr);
        }
        
        return new TwoTierHashSharding(in);        
    }
    
    @Test
    public void hashRingRangeQueryTest(){
        //test with only one shard. 
        TwoTierHashSharding hashRing1= makeSharding(Arrays.asList(new Token[]{TOKENx10}));
        //maps to only token
        AssertTokenMappedToCorrectShard(TOKENx01, hashRing1, TOKENx10, 1);
        //with a wrap around, still should map to only token
        AssertTokenMappedToCorrectShard(TOKENxFF, hashRing1, TOKENx10, 1);
        
        AssertTokenRangeMappedToCorrectShard(TOKENx01, TOKENxFF, hashRing1, TOKENx10, TOKENx10, 1);
        
        // now test with 2 shards
        TwoTierHashSharding hashRing2 = makeSharding(Arrays.asList(new Token[]{TOKENx08, TOKENx10}));
        //maps to first token, no wrap
        AssertTokenMappedToCorrectShard(TOKENx01, hashRing2, TOKENx08, 2);
        //maps to second token, no wrap
        AssertTokenMappedToCorrectShard(TOKENx0B, hashRing2, TOKENx10, 2);
        //maps to first token, wrap needed
        AssertTokenMappedToCorrectShard(TOKENxFF, hashRing2, TOKENx08, 2);
        
        //right on the shard boundary: I am adhering to ( ] type intervals
        AssertTokenMappedToCorrectShard(TOKENx08, hashRing2, TOKENx08, 2);
        
        AssertTokenRangeMappedToCorrectShard(TOKENx01, TOKENx0B, hashRing2, TOKENx08, TOKENx10, 2);
        
        AssertTokenRangeMappedToCorrectShard(TOKENx0B, TOKENxFF, hashRing2, TOKENx10, TOKENx08, 2);
        
        AssertTokenRangeMappedToCorrectShard(TOKENx01, TOKENxFF, hashRing2, TOKENx08, TOKENx10, 2);
                
    }
    
    private void AssertTokenMappedToCorrectShard(Token tok, TwoTierHashSharding ring, Token expected, int expectedReplicaSize){
        Pair<Token, Token> testPair = new Pair<Token, Token>(tok, tok);
        List<Pair<Token, List<Node>>> result = ring.hashRingRangeQuery(testPair);
        Assert.assertEquals(expected, result.iterator().next().getLeft());    
        Assert.assertEquals(1, result.size());
        
        //only one shard implies only one replica, and it should only appear once
        Assert.assertTrue(result.iterator().next().getRight().size() == expectedReplicaSize);
    }
    
    //requires begin.compareTo(includedEnd) <= 0
    private void AssertTokenRangeMappedToCorrectShard(Token begin, Token includedEnd, TwoTierHashSharding ring, Token expectedFirst, Token expectedLast, int expectedResultSize){
        Pair<Token, Token> testPair = new Pair<Token, Token>(begin, includedEnd);
        List<Pair<Token, List<Node>>> result = (List<Pair<Token, List<Node>>>)ring.hashRingRangeQuery(testPair);
        Assert.assertEquals(expectedFirst, result.get(0).getLeft());    
        Assert.assertEquals(expectedLast, result.get(result.size() - 1).getLeft());
        Assert.assertEquals(expectedResultSize, result.size());
    }
    
    
    
    //NOTE: i am assuming a replication level of 3. if that changes, test needs to change
    @Test
    public void hashRingReplicaQueryTest(){

        TwoTierHashSharding hashRing1 = makeSharding(Arrays.asList(new Token[]{TOKENx0B}));
        assertEnoughAndOrderedReplicas(TOKENx01, hashRing1, 1, 0);
        
        
        TwoTierHashSharding hashRing2 = makeSharding(Arrays.asList(new Token[]{TOKENx0B, TOKENx10}));
        assertEnoughAndOrderedReplicas(TOKENx01, hashRing2, 2, 0);
        assertEnoughAndOrderedReplicas(TOKENx0F, hashRing2, 2, 1);
        assertEnoughAndOrderedReplicas(TOKENxFF, hashRing2, 2, 0);

        
        TwoTierHashSharding hashRing3 = makeSharding(Arrays.asList(new Token[]{TOKENx0B, TOKENx10, TOKENx18}));
        assertEnoughAndOrderedReplicas(TOKENx01, hashRing3, 3, 0);
        assertEnoughAndOrderedReplicas(TOKENx0F, hashRing3, 3, 1);
        //boundary
        assertEnoughAndOrderedReplicas(TOKENx10, hashRing3, 3, 1);
        
        TwoTierHashSharding hashRing4 = makeSharding(Arrays.asList(new Token[]{TOKENx0B, TOKENx10, TOKENx18, TOKENxFF}));
        assertEnoughAndOrderedReplicas(TOKENx01, hashRing4, 3, 0);
        assertEnoughAndOrderedReplicas(TOKENx08, hashRing4, 3, 0);
        assertEnoughAndOrderedReplicas(TOKENx10, hashRing4, 3, 1);
        assertEnoughAndOrderedReplicas(TOKENxFF, hashRing4, 3, 3);
    }
    
    private void assertEnoughAndOrderedReplicas(Token tok, TwoTierHashSharding ring, int expectedReplicaSize, int expectedTokenPosition){
        Pair<Token, Token> testPair = new Pair<Token, Token>(tok, tok);
        List<Pair<Token, List<Node>>> result = ring.hashRingRangeQuery(testPair);
        
        //only one shard implies only one replica, and it should only appear once
        List<Node> nodes = result.iterator().next().getRight();
        Iterator<Node> nodeIt = nodes.iterator();
        Assert.assertTrue(nodes.size() == expectedReplicaSize);
        for (int i = 0; i < expectedReplicaSize; i++){
            Assert.assertEquals(nodeIt.next().nodeNumber, ((i + expectedTokenPosition) % ring.getNumShards()));
        }
    }
        
    @Test
    public void testShardingAfterNodeAddress()
    {
        Vertex v = new Vertex(1);
        HierarchicalHashFunction hashfun = new HierarchicalHashFunction();        
        Pair<Token,Token> pt = hashfun.hash(v); 
        List<Pair<Shard, Collection<Node>>> answer = tths.getShardForVertexQuery(v);

        //must have exactly one node for this example
        Assert.assertTrue(answer.size()  ==  1);     
    }
        
    private List<Vertex> generateThisManyVertices(int howMany){
        List<Vertex> answer = new ArrayList<Vertex>(howMany);
     
        for (int i = 0; i < howMany; i++){
            answer.add(i, new Vertex(i));
        }
        
        return answer;
    }
    
    private Map<Node, IDataNode> generateThisManyNodes(int howMany){
        Map<Node, IDataNode> nodes = new HashMap<Node, IDataNode>(howMany);
        
        //TODO: this is only useful for its keys, but need to obey the types, 
        //and I don't want to revert/change that atm.
        for (int i = 0; i < howMany; i++){
            nodes.put(new Node(i), null);
        }        
        
        return nodes;
    }
    
    private List<Edge> generateAllEdgeCombinations(List<Vertex> vertices){
        List<Edge> answer = new ArrayList<Edge>(vertices.size()*vertices.size());
        
        for (Vertex outer : vertices){
            for (Vertex inner : vertices){
                answer.add(new Edge(outer, inner));
            }
        }
        
        return answer;
    }
    

    private void assertEdgeShardConsistentWithVertexShard(TwoTierHashSharding sharding, List<Vertex> vertices){

        List<Edge> edges = generateAllEdgeCombinations(vertices);
        
        for (Edge e: edges){            
            Pair<Shard, Collection<Node>> edgequeryresult = sharding.getShardForEdgeQuery(e);
            List<Pair<Shard, Collection<Node>>> vertexqueryresult = sharding.getShardForVertexQuery(e.getLeftEndpoint());
            
            Assert.assertTrue(vertexqueryresult.contains(edgequeryresult));

        }
    }
    
    @Test public void testInitialSharding()
    {   
        List<Vertex> verticesForQuerying = generateThisManyVertices(20);
        List<Vertex> noExceptions = generateThisManyVertices(0);
        
        
        //checks that the shard in charge of an edge is contained in the charges returned for the full vertex.
        TwoTierHashSharding tths0 = new TwoTierHashSharding(generateThisManyVertices(0), generateThisManyNodes(1), 1, 0, 0);
            assertEdgeShardConsistentWithVertexShard(tths0, verticesForQuerying);
        
        TwoTierHashSharding tths1 = new TwoTierHashSharding(generateThisManyVertices(0), generateThisManyNodes(1), 10, 0, 0);
        assertEdgeShardConsistentWithVertexShard(tths1, verticesForQuerying);
        
        TwoTierHashSharding tths2 = new TwoTierHashSharding(generateThisManyVertices(0), generateThisManyNodes(1), 100000, 0, 0);
        assertEdgeShardConsistentWithVertexShard(tths1, verticesForQuerying);
    }
    
    //tests the sharding lib spreads edges evenly (when there are many edges and relatively few shards)
    @Test public void testEvenOrdinaryPartition(){    
    	
    	SamplableBuilder sb = new SamplableBuilder();
    	sb.setType(DistributionType.CONSTANT);
    	sb.setValue(1);
    	
        GraphParameters gp = new GraphParameters(100000, sb.build());
        
        int numExceptions = 0;
        int numOrdinaryShards = 10; //small so that split is even
        int numNodes = 5; //ditto
        int numShardsPerException = 0; //shouldn't matter
        int numNodesPerException = 0; //shouldn'matter
        //TODO: convert to builder
        TwoTierHashSharding sharding = TwoTierHashSharding.makeTwoTierHashFromNumExceptions(
                numExceptions,
                generateThisManyNodes(numNodes), 
                numOrdinaryShards, 
                numShardsPerException, 
                numNodesPerException);
        
        assertBalancedByShardAndByStorageNode(gp, sharding, numOrdinaryShards, numNodes);        
    }
    
    //tests that stuff is spread out evenly within an exceptions' shards
    @Test public void testEvenExceptionPartition(){
          
     	SamplableBuilder sb = new SamplableBuilder();
    	sb.setType(DistributionType.CONSTANT);
    	sb.setValue(100000);
    	
        GraphParameters gp = new GraphParameters(5, sb.build());
                    
          int numExceptions = 5; // a few exceptions
          int numOrdinaryShards = 1; // compulsory
          int numShardsPerException = 10;// relatively few compared to number of edges
          int numNodesPerException = 10;//ditto
          int numNodes = numNodesPerException; //should be at least as big as numNodesPerException
          TwoTierHashSharding sharding = TwoTierHashSharding.makeTwoTierHashFromNumExceptions(
                  numExceptions,
                  generateThisManyNodes(numNodes), 
                  numOrdinaryShards, 
                  numShardsPerException, 
                  numNodesPerException);

          assertBalancedByShardAndByStorageNode(gp, sharding, numExceptions*numShardsPerException, numNodes);
      }
    
    private void assertBalancedByShardAndByStorageNode(GraphParameters gp, ISharding sharding, int numShards, int numNodes){
        Graph gr = SkewedDegreeGraph.makeSkewedDegreeGraph(gp);
        Iterator<Edge> it = gr.graphIterator();
        
        MapBackedCounter<Shard> counterByShard = new MapBackedCounter<Shard>();
        MapBackedCounter<Node> counterByNode = new MapBackedCounter<Node>();
        while (it.hasNext()){
            Edge nextEdge = it.next();
            counterByShard.increaseCount(sharding.getShardForEdgeQuery(nextEdge).getLeft());
            counterByNode.increaseCount(sharding.getShardForEdgeQuery(nextEdge).getRight().iterator().next());
        }
        
        //these can fail for some parameter combos (eg if they are small), but should 
        //always succeed given large enough numbers of edges
        assertBalanced(gr.getActualDegree(), numShards, 0.1, counterByShard);
        assertBalanced(gr.getActualDegree(), numNodes, 0.1, counterByNode);                
    }
    
    //TODO: use general Counter interface
    public static <T> void assertBalanced(int numParticles, int numBins, double maxSpreadTolerated, MapBackedCounter<T> tally){
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
    
        Assert.assertEquals(numBins, tally.entrySet().size());
        
        for (Map.Entry<T, Integer> ent: tally.entrySet()){
            min = Math.min(min, ent.getValue());
            max = Math.max(max, ent.getValue());
        }
        
        Assert.assertTrue(min > 0);
        Assert.assertTrue(max >= min);
        
        double spread = ((double)(max - min))/min;
        Assert.assertTrue(String.format("min: %d, max: %d, bins: %d, actual: %f, max: %f", min, max, numBins, spread, maxSpreadTolerated), spread <= maxSpreadTolerated);
        Assert.assertEquals(numParticles, tally.getTotal());
        
    }
    


}