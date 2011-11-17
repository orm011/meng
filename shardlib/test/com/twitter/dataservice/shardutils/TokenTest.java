package com.twitter.dataservice.shardutils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.Assert;

import org.junit.Test;

public class TokenTest
{
    
    final byte zerozero = (byte) 0x00;
    final byte zeroone = (byte) 0x01;
    final byte zeroeff = (byte) 0x0f;
    final byte eightzero = (byte) 0x80;        
    final byte effeff = (byte) 0xff;

    final byte[] emptyarr0 = new byte[]{};
    final byte[] emptyarr1 = new byte[]{};
        
    //make sure the array is an increasing sequence, it is necessary for test to work
    final byte[] significantCases = new byte[] {zerozero, zeroone, zeroeff, eightzero, effeff};
    
    private void AssertReflexivity(Token token1){
        Token tok1copy = new Token(token1.position.toByteArray());
        
        //expects reflexivity and consistency among comparison and equals
        Assert.assertEquals(token1, tok1copy);
        Assert.assertEquals(token1.compareTo(tok1copy), 0);    
    }
    
    private void AssertSymmetry(Token tok1, Token tok2){
        //expected symmetry
        Assert.assertEquals(Integer.signum(tok1.compareTo(tok2)), 
                -Integer.signum(tok2.compareTo(tok1)));
    }
    
    private void AssertEqualsConsistency(Token tok1, Token tok2){
        //expected consistency with .equals() (both ways)
        if (tok1.compareTo(tok2) == 0)
            Assert.assertEquals(tok1, tok2);                
        if (tok1.equals(tok2))
            Assert.assertEquals(tok1.compareTo(tok2), 0);        
    }
    
 
    @Test
    public void testBasicOrderContract(){    
        //zero length comparisons
        Assert.assertTrue(new Token(emptyarr0).equals(new Token(emptyarr1)));
        Assert.assertTrue(new Token(emptyarr0).compareTo(new Token(emptyarr1)) == 0);
        
        byte[] arr1;
        byte[] arr2;
        //length one comparisons
        for (int i = 0; i < significantCases.length; i++){
            Token tok1 =  new Token(new byte[]{significantCases[i]});            
            AssertReflexivity(tok1);            
            
            for (int j = i; j < significantCases.length; j++){
                Token tok2 = new Token(new byte[]{significantCases[j]});                
                AssertSymmetry(tok1, tok2);
                
                //expected correct value. also implicitly transitivity in this case
                Assert.assertEquals(Integer.signum(tok1.compareTo(tok2)),
                        Integer.signum(i - j));

                AssertEqualsConsistency(tok1, tok2);
            }            
        }
        
        //length two comparisons
        for (int i = 0; i < significantCases.length; i++){
            for (int j = 0; j < significantCases.length; j++){
                Token token1 = new Token(new byte[]{significantCases[i], significantCases[j]});                
                
                AssertReflexivity(token1);
                //only need to consider cases starting with k = i, the rest go by the second assertion
                for (int k = i; k < significantCases.length; k++){
                    for (int l = 0; l < significantCases.length; l++){
                            Token token2 = new Token(new byte[] {significantCases[k], significantCases[l]});
                            
                            AssertSymmetry(token1, token2);
                            AssertEqualsConsistency(token1, token2);
 
                            int expected = Integer.signum((i*significantCases.length + j) -
                                    (k*significantCases.length + l));
                            
                            Assert.assertEquals(expected, Integer.signum(token1.compareTo(token2)));
                    }
                }                
            }            
        }
        
        Token token1 = new Token(new byte[]{});
        Token token2 = new Token(new byte[]{effeff});
        Assert.assertFalse(token1.equals(token2));
        Assert.assertFalse(token2.equals(token1));
         
    }   

    @Test
    public void testSort(){
        Token[] tokenArray1 = new Token[significantCases.length];
        Token[] tokenArray2 = new Token[significantCases.length];

        int i = 0;
        for (byte mybyte: significantCases){
            tokenArray2[tokenArray2.length - i - 1] = new Token(new byte[]{mybyte});
            tokenArray1[i] = new Token(new byte[]{mybyte});
            i++;
        }
        
        //tokenArray3  and tokenArray2 are not sorted        
        Token[] tokenArray3 = Arrays.copyOf(tokenArray2, tokenArray2.length);
        Arrays.sort(tokenArray2);
        Assert.assertTrue(Arrays.equals(tokenArray1, tokenArray2));
        Assert.assertFalse(Arrays.equals(tokenArray3, tokenArray2));
        
        //now try the sorted set
        SortedSet<Token> mySortedSet = new TreeSet<Token>(Arrays.asList(tokenArray3));
        
        Iterator<Token> fromSet = mySortedSet.iterator();
        Iterator<Token> fromArray = Arrays.asList(tokenArray1).iterator();
        
        while (fromSet.hasNext() && fromArray.hasNext()){
            Assert.assertEquals(fromSet.next(), fromArray.next());
        }
        
        Assert.assertEquals(fromSet.hasNext(), fromArray.hasNext());
        
        //now add the same elements, nothing should change (comparison should detect equality)
        mySortedSet.addAll(Arrays.asList(tokenArray3));
        
        Iterator<Token> iterateAgain = mySortedSet.iterator();
        fromArray = Arrays.asList(tokenArray1).iterator();
        while (iterateAgain.hasNext() && fromArray.hasNext()){
            Assert.assertEquals(iterateAgain.next(), fromArray.next());
        }
        Assert.assertEquals(iterateAgain.hasNext(), fromArray.hasNext());
                
    }
    
    //tests I can recover the byteRep well even though I store it as a BigInt
    @Test public void byteRepTest(){
        for (byte b: significantCases){
            Assert.assertTrue(Arrays.equals(new byte[]{b}, new Token(new byte[]{b}).byteRep()));
        }
        
        Assert.assertTrue(Arrays.equals(significantCases, new Token(significantCases).byteRep()));   
    }
    
    private void assertTokenArrayEquals(byte[][] expected, List<Token> tokens){
        Assert.assertEquals(expected.length, tokens.size());

        Object[] toks = tokens.toArray();
        for (int i = 0; i < toks.length; i++){
            Assert.assertEquals(new Token(expected[i]), toks[i]);
        }
    }

    //why is it so hard to make an array in Java.
    
    private byte[] makeArr(int first, int second, int third, int fourth){
        return new byte[]{(byte) first, (byte) second, (byte) third, (byte) fourth};
    }
    private byte[] makeArr(int first, int second, int third){
        return new byte[]{(byte) first, (byte) second, (byte) third};
    }
    private byte[] makeArr(int first, int second){
        return new byte[]{(byte) first, (byte) second};
    }
    
    private byte[] makeArr(int first){
        return new byte[]{(byte) first};
    }
    
    @Test public void newSplitTest(){
        byte[][] expected1 = new byte[][]{
                makeArr(0x00, 0x40),
                makeArr(0x00, 0x80),
                makeArr(0x00, 0xc0),
                makeArr(0x00, 0xff)
        };
        
        byte[][] expected1b = new byte[][]{
                makeArr(0x00, 0x55),
                makeArr(0x00, 0xaa),
                makeArr(0x00, 0xff)
        };
        
        byte[][] expected1c = new byte[][]{
                makeArr(0x00, 0x80),
                makeArr(0x00, 0xff)
        };
        
        byte[][] expected1d = new byte[][]{
                makeArr(0x00, 0xff)
        };
        
        List<Token> ans1 = Token.splitPrefixImpliedRange(makeArr(0x00), 2, 1, 4);
        assertTokenArrayEquals(expected1, ans1);
        List<Token> ans1b = Token.splitPrefixImpliedRange(makeArr(0x00), 2, 1, 3);
        assertTokenArrayEquals(expected1b, ans1b);
        List<Token> ans1c = Token.splitPrefixImpliedRange(makeArr(0x00), 2, 1, 2);
        assertTokenArrayEquals(expected1c, ans1c);
        List<Token> ans1d = Token.splitPrefixImpliedRange(makeArr(0x00), 2, 1, 1);
        assertTokenArrayEquals(expected1d, ans1d);
        
        byte[][] expected2 = new byte[][]{
                makeArr(0x40, 0xff),
                makeArr(0x80, 0xff),
                makeArr(0xc0, 0xff),
                makeArr(0xff, 0xff)
        };
        List<Token> ans2 = Token.splitPrefixImpliedRange(new byte[]{}, 2, 1, 4);
        assertTokenArrayEquals(expected2, ans2);

        byte[][] expected3 = new byte[][]{
                makeArr(0x40, 0x00, 0xff, 0xff),
                makeArr(0x80, 0x00, 0xff, 0xff),
                makeArr(0xc0, 0x00, 0xff, 0xff),
                makeArr(0xff, 0xff, 0xff, 0xff)
        };
        List<Token> ans3 = Token.splitFullTokenSpace(2, 4);
        assertTokenArrayEquals(expected3, ans3);
                
        byte[][] expected4 = new byte[][]{
                makeArr(0x11, 0x40),
                makeArr(0x11, 0x80),
                makeArr(0x11, 0xc0),
                makeArr(0x11, 0xff)
        };
        List<Token> ans4 = Token.splitNodeImpliedRange(new Token(new byte[]{0x11}), 4);
        assertTokenArrayEquals(expected4, ans4);
        
        byte[][] expected5 = new byte[][]{
                makeArr(0x11, 0x40, 0xff),
                makeArr(0x11, 0x80, 0xff),
                makeArr(0x11, 0xc0, 0xff),
                makeArr(0x11, 0xff, 0xff),
        };
        List<Token> ans5 = Token.splitPrefixImpliedRange(new byte[]{(byte) 0x11}, 3, 1, 4);
        assertTokenArrayEquals(expected5, ans5);
    }
    
    @Test
    public void predecessorTest(){
        Token test1 = new Token(makeArr(0x00));
        Assert.assertEquals(new Token(makeArr(0xff)), test1.predecessor());
        
        Token test2 = new Token(makeArr(0x11, 0x00));
        Assert.assertEquals(new Token(makeArr(0x10, 0xff)), test2.predecessor());
        
    }
  
}