package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author 城北徐公
 * @Date 2023/10/16-9:12
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensorWM {
    private String id;
    private Double vc;
    private String start;
    private String end;
}
