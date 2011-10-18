package com.twitter.dataservice.shardutils;

import java.util.Arrays;
import java.util.Iterator;
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
    
    
    //not really a unit test, but more an exploration test. delete later.
    @Test
    public void splitTest(){
        
        Token ZERO = new Token(new byte[]{zerozero});
        Token FF = new Token (new byte[]{effeff});

        
        try {
            List<Token> toklist = Token.split(ZERO, ZERO, 1);
            Assert.fail();
        } catch (IllegalArgumentException e){}
        
        List<Token> secondTry = Token.split(ZERO, FF, 2);
        Assert.assertEquals(2, secondTry.size());
        Token EXPECTED = new Token(new byte[]{(byte) 0x80});
        Assert.assertEquals(EXPECTED, secondTry.get(0));
        Assert.assertEquals(FF, secondTry.get(1));
    }
        
}