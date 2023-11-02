package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author 城北徐公
 * @Date 2023/10/14-8:55
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensorWin extends WaterSensor {

    private String id;
    private Long ts;
    private Double vc;
    private String start;
    private String end;

}
