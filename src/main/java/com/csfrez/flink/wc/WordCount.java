package com.csfrez.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理WordCount
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读数数据
        String inputPath = "D:\\Github\\csfrez\\demo-flink\\src\\main\\resources\\hello.txt";
        DataSource<String> dataSource = executionEnvironment.readTextFile(inputPath);

        // 对数据集进行处理，按空格分词展开，转换成（word, 1）二元组进行统计
        DataSet<Tuple2<String, Integer>> resultSet = dataSource.flatMap(new MyFlatMapFunction())
                .groupBy(0) // 按照第一個位置的word分组
                .sum(1);// 将第二个位置上的数据求和
        // 打印结果
        resultSet.print();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按空格分词
            String[] words = value.split(" ");
            for(String word : words){
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
