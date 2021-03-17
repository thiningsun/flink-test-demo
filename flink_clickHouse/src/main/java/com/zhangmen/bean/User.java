package com.zhangmen.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class User {
    private String id;
    private String userName;
    private String age;
    private String sex;
    private String time;
}