package com.atguigu.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Author 城北徐公
 * @Date 2023/10/24-20:32
 */
//ROW<word1 STRING, word2 STRING> 每一行数据炸裂出来的行数
@FunctionHint(output = @DataTypeHint("ROW<word1 STRING, word2 STRING>"))
public class MyUDTF extends TableFunction<Row> {
    //str:hello_atguigu-hello_flink
    public void eval(String str) {
        String[] split = str.split("-"); //拆分出行
        for (String s : split) {
            String[] words = s.split("_"); //拆分出列
            collect(Row.of(words[0],words[1]));
        }
    }

}
