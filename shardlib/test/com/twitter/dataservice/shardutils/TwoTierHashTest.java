package com.twitter.dataservice.shardutils;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.twitter.dataservice.sharding.TwoTierHashSharding;

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
        TwoTierHashSharding.CycleIterator<Token> single = new TwoTierHashSharding.CycleIterator<Token>(sizeOneSet, szOneIter);
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
        
        TwoTierHashSharding.CycleIterator<Token> at = new TwoTierHashSharding.CycleIterator<Token>(sizeTwoSet, sizeTwoSet.iterator());
        TwoTierHashSharding.CycleIterator<Token> et = new TwoTierHashSharding.CycleIterator<Token>(sizeTwoSet);
        TwoTierHashSharding.CycleIterator<Token> it = new TwoTierHashSharding.CycleIterator<Token>(sizeTwoSet, usedIter);
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
        TwoTierHashSharding.CycleIterator<Token> cycle1 = new TwoTierHashSharding.CycleIterator<Token>(tokenset);
        TwoTierHashSharding.CycleIterator<Token> cycle2 = new TwoTierHashSharding.CycleIterator<Token>(tokenset);
        TwoTierHashSharding.CycleIterator<Token> cycle3 = new TwoTierHashSharding.CycleIterator<Token>(tokenset);
        
        Iterator<List<Token>> windowcycle1 = new TwoTierHashSharding.SlidingWindowCycleIterator<Token>(cycle1, 1);       
        Iterator<List<Token>> windowcycle2 = new TwoTierHashSharding.SlidingWindowCycleIterator<Token>(cycle2, 2); 
        Iterator<List<Token>> windowcycle4 = new TwoTierHashSharding.SlidingWindowCycleIterator<Token>(cycle3, 4);
        
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
        
//    @Test
//    public void testShardingAfterNodeAddress()
//    {
//        Vertex v = new Vertex(1);
//        TwoTierHashSharding.HierarchicalHashFunction hashfun = new TwoTierHashSharding.HierarchicalHashFunction();        
//        Pair<Token,Token> pt = hashfun.hash(v); 
//        
//        List<Pair<Shard, Collection<Node>>> answer = tths.getShardForVertexQuery(v);
//        
//        //must have exactly one node for this example
//        Assert.assertTrue(answer.size()  ==  1);
//        for (Pair<Shard, Collection<Node>> p: answer)
//            Assert.assertTrue(p.getLeft().getLowerEnd().compareTo(pt.getLeft()) >= 0 
//                        || p.getLeft().getLowerEnd().equals(new Token(bytez)));        
//    }
    

}