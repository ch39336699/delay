package com.fedex.intl.cd.utils;

public abstract class MessageBase<T> {

  public abstract T decodeMessage(byte[] message);

}
