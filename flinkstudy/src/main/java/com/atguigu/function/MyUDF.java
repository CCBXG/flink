package com.atguigu.function;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author 城北徐公
 * @Date 2023/10/24-19:19
 */
public class MyUDF extends ScalarFunction {
    //@DataTypeHint(inputGroup = InputGroup.ANY)
    public String eval( String lower) {
        return lower.toUpperCase();
    }

}
