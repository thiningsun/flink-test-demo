package com.zhangmen.filter;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TODO 基于布隆过滤器来进行数据去重
 */
public class BloomFilter {
    public static final int NUM_SLOTS=1024*1024*8;
    public static final int NUM_HASH=8;
    private BigInteger bitmap = new BigInteger("0");

    public static void main(String[] args) {
        //测试代码
        BloomFilter bf = new BloomFilter();

        ArrayList<String> contents = new ArrayList<>();
        contents.add("sldkjelsjf");
        contents.add("ggl;ker;gekr");
        contents.add("wieoneomfwe");
        contents.add("sldkjelsvrnlkjf");
        contents.add("ksldkflefwefwefe");
        contents.add("212");

        for(int i=0;i<contents.size();i++){
            bf.addElement(contents.get(i));
        }

        if (!bf.check("sda")) {
            bf.addElement("sda");
        }

        System.out.println(bf.check("sldkjelsvrnlkjf"));
        System.out.println(bf.check("sldkjelsvrnkjf"));
        System.out.println(bf.check("212"));
    }

    public void addElement(String message){

        for(int i=0;i<NUM_HASH;i++){
            int hashcode = getHash(message,i);
            if(!bitmap.testBit(hashcode)){
                bitmap = bitmap.or(new BigInteger("1").shiftLeft(hashcode));
            }
        }

    }

    private int getHash(String message,int n){
        try {
            MessageDigest md5 = MessageDigest.getInstance("md5");
            message  = message +String.valueOf(n);
            byte[] bytes = message.getBytes();
            md5.update(bytes);
            BigInteger bi = new BigInteger(md5.digest());

            return Math.abs(bi.intValue())%NUM_SLOTS;
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(BloomFilter.class.getName()).log(Level.SEVERE, null, ex);
        }
        return -1;
    }

    public boolean check(String message){
        for(int i=0;i<NUM_HASH;i++){
            int hashcode = getHash(message,i);
            if(!this.bitmap.testBit(hashcode)){
                return false;
            }
        }
        return true;
    }
}
