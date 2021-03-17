/*
package com.zhangmen.filter;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.nio.charset.Charset;

public class GoogleBloomFilter {
    public static void main(String[] args) {
        BloomFilter<String> filter =
                BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), 1000);
        filter.put("www.baidu.com");
        filter.put("www.163.com");

        System.out.println(filter.mightContain("www.126.com"));
        System.out.println(filter.mightContain("www.163.com"));
        System.out.println(filter.mightContain("www.baidu.com"));
    }
}
*/
