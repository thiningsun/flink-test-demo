package com.zhangmen.bean;


import lombok.Data;

import javax.inject.Singleton;
import java.util.ArrayList;

@Data
@Singleton
public class Item {
    private Integer id;
    private String name;
    private String flage;

}
