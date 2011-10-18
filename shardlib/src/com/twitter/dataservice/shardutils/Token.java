package com.twitter.dataservice.shardutils;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class Token implements Comparable<Token>
{
  final BigInteger position;
  
  public Token(byte[] bytes) {
    this.position = new BigInteger(1, bytes);
  }
  
  private Token(BigInteger biggie) {
      this.position = biggie;
  }
  
  @Override
  public String toString(){
      byte[] bytes = position.toByteArray();
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
  

  public static List<Token> split(Token first, Token last, int numSplits){
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
              
              answer.add(new Token(previous));
          }
          
          return answer;
  }
  
}
