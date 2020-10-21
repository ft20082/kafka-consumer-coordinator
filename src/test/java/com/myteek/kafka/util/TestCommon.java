package com.myteek.kafka.util;

import org.junit.Assert;
import org.junit.Test;

public class TestCommon {

    @Test
    public void testBytesLongConvert() {
        long test = 12349234L;
        byte[] bytes = Common.longToBytes(test);
        long newTest = Common.bytesToLong(bytes);
        System.out.println("long: " + test);
        System.out.println("bytes: " + bytes);
        System.out.println("long: " + newTest);
        Assert.assertEquals(test, newTest);
    }

    @Test
    public void testBytesIntConvert() {
        int test = 12349234;
        byte[] bytes = Common.intToBytes(test);
        int newTest = Common.bytesToInt(bytes);
        System.out.println("int: " + test);
        System.out.println("bytes: " + bytes);
        System.out.println("int: " + newTest);
        Assert.assertEquals(test, newTest);
    }

}
