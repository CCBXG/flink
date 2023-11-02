package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author 城北徐公
 * @Date 2023/10/17-10:33
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VcAcc {
    private String id;
    private Double sumVc;
    private Integer countVc;
}
