package com.twitter.dataservice.shardlib;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Arrays;

public class Token
{
  final byte[] bytes;

  public Token(byte[] bytes) {
    this.bytes = Arrays.copyOf(bytes,bytes.length);
  }

  public boolean greaterThan(Token other){
    //    int pos = 0;
    //    while ((pos < other.bytes.length) && (pos < bytes.length) && other[pos] ){
    //
    //    }
    throw new NotImplementedException();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    Token token = (Token) o;

    if (!Arrays.equals(bytes, token.bytes)) { return false; }

    return true;
  }

  @Override
  public int hashCode() {
    return bytes != null ? Arrays.hashCode(bytes) : 0;
  }
  
}
