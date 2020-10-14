package com.zhangmen.filter;

import java.util.LinkedHashMap;
import java.util.Map;
// 继承LinkedHashMap
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int MAX_CACHE_SIZE;

    public LRUCache(int cacheSize) {
        // 使用构造方法 public LinkedHashMap(int initialCapacity, float loadFactor, boolean accessOrder)
        // initialCapacity、loadFactor都不重要
        // accessOrder要设置为true，按访问排序
        super((int) Math.ceil(cacheSize / 0.75) + 1, 0.75f, true);
        MAX_CACHE_SIZE = cacheSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
        // 超过阈值时返回true，进行LRU淘汰
        return size() > MAX_CACHE_SIZE;
    }

    public static void main(String[] args) {
        LRUCache<Integer, String> lruCache = new LRUCache(100);
        for (int i = 0; i < 150; i++) {
            lruCache.put(i, "zhangsan" + i);
        }
        lruCache.put(1, "zhansan1");
        System.out.println(lruCache.size());
        for (Map.Entry<Integer, String> entry : lruCache.entrySet()) {
            Integer key = entry.getKey();
            String value = entry.getValue();
            System.out.println(key + "==" + value);
        }
    }

}