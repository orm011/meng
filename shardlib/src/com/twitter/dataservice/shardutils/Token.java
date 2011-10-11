package com.twitter.dataservice.shardutils;

import java.util.Arrays;

public class Token implements Comparable<Token>
{
  final byte[] bytes;

  public Token(byte[] bytes) {
    this.bytes = Arrays.copyOf(bytes,bytes.length);
  }
  
  @Override
  public String toString(){
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
    return this.bytes.length == token.bytes.length && this.compareTo(token) == 0;
  }

  @Override
  public int hashCode() {
    return bytes != null ? Arrays.hashCode(bytes) : 0;
  }
  
  private int byteAsPositiveInteger(byte b){
      return ((int) b) & 0xff;
  }
  
  @Override
  public int compareTo(Token other){
     
      //I'm using the ClassCastException for cases I consider incomparable
      if (other.bytes.length != this.bytes.length){
          throw new ClassCastException("Tokens of different lengths are not Comparable");
      }
      
      if (this.bytes.length == 0)
          return 0;
      
      int prev = 0, pos = 0;
      
      while (pos < other.bytes.length && pos < bytes.length){
          prev = pos++;              
          
          if ((this.bytes[prev] - other.bytes[prev]) != 0)
              break;          
      }
      
      // loop breaks and the last read bytes were unequal
      // need to transform to positive integer so that difference is positive in cases such as 0xff vs. 0x00.
      // otherwise we get behavior such as 0x01 > 0x00, which is not intuitive when we put
      // more bytes in an array eg 0x0000 > 0x0010.
      return byteAsPositiveInteger(this.bytes[prev]) - byteAsPositiveInteger(other.bytes[prev]);
      
  }
  
}
