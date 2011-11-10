package com.twitter.dataservice.shardutils;

public interface IByteSerializable
{
    //returns a byte array that should be unique 
    //for every different value, ie
    //for every value of a type, and among types
    public byte[] toByteArray();
}
