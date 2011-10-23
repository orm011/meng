package com.twitter.dataservice.shardutils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

public class Token implements Comparable<Token>
{
    //hash word size in bytes
  public static int DEFAULT_PREFIX_LENGTH = (128 >>> 3);

  final int size;
  final BigInteger position;
  
  public Token(byte[] bytes) {
    this.position = new BigInteger(1, bytes);
    this.size = bytes.length;
  }
  
  public byte[] byteRep(){
      byte[] rep = new byte[size];
      byte[] fromBigInt = position.toByteArray();
      int index = (fromBigInt[0] != 0)? 0:1;
      if (index > 0) assert (fromBigInt.length == size + 1);
      int copySize = fromBigInt.length - index;
      System.arraycopy(fromBigInt, index, rep, rep.length - copySize, copySize);      
      return rep;
  }
  
  
  private Token(BigInteger biggie, int size) {
      this.position = biggie;
      this.size = size;
  }
  
  @Override
  public String toString(){
      byte[] bytes = this.byteRep();
      System.out.println(this.byteRep().length);
      StringBuilder sb = new StringBuilder(2*bytes.length);
      for (byte b: bytes){
          sb.append(String.format("%02x", b));
      }
      
      return "Token: " + sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    Token token = (Token) o;
    return this.position.equals(token.position);
  }

  @Override
  public int hashCode() {
      byte[] bytes = position.toByteArray();
      return bytes != null ? Arrays.hashCode(bytes) : 0;
  }
  
  private int byteAsPositiveInteger(byte b){
      return ((int) b) & 0xff;
  }
  
  @Override
  public int compareTo(Token other){
      return this.position.compareTo(other.position);
  }
  
  private Token concat(Token right){
      byte[] answer = new byte[this.byteRep().length + right.byteRep().length];
      
      System.arraycopy(this.byteRep(), 0, answer, 0, this.byteRep().length);
      System.arraycopy(right.byteRep(), 0, answer, this.byteRep().length, right.byteRep().length);
      
      return new Token(answer);
  }
  
  
  private static byte[] getEffArray(int size){
      byte[] answer = new byte[size];
      Arrays.fill(answer, (byte)0xff);
      return answer;
  }
  
  
  
  public static List<Token> splitFullTokenSpace(int prefixSize, int numSplits){
      return splitPrefixImpliedRange(new byte[0], 2*prefixSize, prefixSize, numSplits);
  }
  
  public static List<Token> splitNodeImpliedRange(Token v, int numSplits){
      return splitPrefixImpliedRange(v.byteRep(), 2*(v.byteRep().length), (v.byteRep().length), numSplits);
  }
  
  /* 
   * @param prefix: every  token generated has that prefix
   * @param byteSize: every token generated will has this size
   * @param numChangingBytes: the actual varying component in this partition
   * if it is too small with respect to the number of splits. the method will throw an IllegalArgumentException
   * note byteSize = prefixSize + numChangingBytes + Eff filler, so the numChangingBytes needs to be small.
   * @param numSplits: the number of parts to split this into.
   */
  public static List<Token> splitPrefixImpliedRange(byte[] prefix, int byteSize, int numChangingBytes, int numSplits){
      Token begin = new Token(new byte[numChangingBytes]); //0s  
      Token end = new Token(getEffArray(numChangingBytes));//fs
      
      List<Token> toks = split(begin, end, numSplits);
      List<Token> answer = new ArrayList<Token>(toks.size());
      
      Token prefixToken = new Token(prefix);
      Token effToken = new Token(getEffArray(byteSize - numChangingBytes - prefix.length));
      
      for (Token splittingToken: toks){
          answer.add(prefixToken.concat(splittingToken).concat(effToken));
      }
      
      return answer;
  }
  
  private static List<Token> split(Token first, Token last, int numSplits){
          BigInteger difference = last.position.subtract(first.position);
          
          if (numSplits < 1 || (difference.compareTo(BigInteger.valueOf(numSplits)) < 0))
              throw new IllegalArgumentException();
          
          BigInteger[] quotientAndRemainder = difference.divideAndRemainder(BigInteger.valueOf(numSplits));
          BigInteger quotient = quotientAndRemainder[0];
          BigInteger remainder = quotientAndRemainder[1];
          
          List<Token> answer = new LinkedList<Token>();
          
          BigInteger previous = first.position;
          BigInteger quotientPlus1 = quotient.add(BigInteger.ONE);
          for (int i = 0; i < numSplits; i++){
              if (remainder.compareTo(BigInteger.valueOf(i)) > 0){
                  previous = previous.add(quotientPlus1);
              } else {
                  previous = previous.add(quotient);
              }
              
              answer.add(new Token(previous, first.size));
          }
          
          return answer;
  }
  
}



