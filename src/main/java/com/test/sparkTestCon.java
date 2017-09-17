package com.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by wangfeng on 2017/9/13.
 */
public class sparkTestCon {
    public static void main(String[] args) {
       // String home = System.getProperty("hadoop.home.dir");
 /*       System.setProperty("hadoop.home.dir", "192.168.1.138");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        System.setProperty("HADOOP_HOME", "hadoop");
*/
/*        SparkConf conf=new SparkConf();
        conf.set("spark.testing.memory", "2147480000");     //因为jvm无法获得足够的资源
        conf.setMaster("spark://192.168.1.138:7077");
        conf.setAppName( "First Spark App" );
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println(sc);*/

        SparkConf conf = new SparkConf();
        conf.setAppName("WordCounter");
        conf.setMaster("spark://192.168.1.138:7077");

        String fileName = System.getProperty("user.dir")+"/src/main/resources/sparktest.log";
System.out.println("========="+fileName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(fileName, 1);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
                    private static final long serialVersionUID = 1L;
                    // 以前的版本好像是Iterable而不是Iterator
                   // @Override
                    public Iterator<String> call(String line) throws Exception {
                        return Arrays.asList(line.split(" ")).iterator();
                    }
                });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = 1L;
                   // @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });
        JavaPairRDD<String, Integer> result = pairs.reduceByKey( new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = 1L;
                    //@Override
                    public Integer call(Integer e, Integer acc) throws Exception {
                        return e + acc;
                    }
                }, 1);

        result.map(new Function<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    private static final long serialVersionUID = 1L;
                    //@Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> v1) throws Exception {
                        return new Tuple2(v1._1, v1._2);
                    }
                }).sortBy(new Function<Tuple2<String, Integer>, Integer>() {
                    private static final long serialVersionUID = 1L;
                   // @Override
                    public Integer call(Tuple2<String, Integer> v1) throws Exception {
                        return v1._2;
                    }
                }, false, 1);
        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    private static final long serialVersionUID = 1L;
                    //@Override
                    public void call(Tuple2<String, Integer> e) throws Exception {
                        System.out.println("【" + e._1 + "】出现了" + e._2 + "次");
                    }
                });
        sc.close();
    }
}
