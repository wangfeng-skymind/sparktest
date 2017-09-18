package com.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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


        System.out.println("spark home:"+sc.getSparkHome());

        JavaRDD<String> lines = sc.textFile(fileName, 1);
        lines.cache();
        List<String> line = lines.collect();
        for ( String val:line ) {
            System.out.println(val);
        }
        //下面这些也是RDD的常用函数
        // lines.collect();  List<String>
        // lines.union();     javaRDD<String>
        // lines.top(1);     List<String>
        // lines.count();      long
        // lines.countByValue();


        /**
         *   filter test
         *   定义一个返回bool类型的函数，spark运行filter的时候会过滤掉那些返回只为false的数据
         *   String s，中的变量s可以认为就是变量lines（lines可以理解为一系列的String类型数据）的每一条数据
         */
        JavaRDD<String> contaninsE = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return (s.contains("they"));
            }
        });
        System.out.println("--------------next filter's  result------------------");
        line = contaninsE.collect();
        for ( String val : line ) {
            System.out.println ( val );
        }
        /**
         * sample test
         * sample函数使用很简单，用于对数据进行抽样
         * 参数为：withReplacement: Boolean, fraction: Double, seed: Int
         *
         */
        JavaRDD<String> sampletest = lines.sample(false,0.1,5);
        System.out.println("-------------next sample-------------------");
        line = sampletest.collect();
        for(String val:line) {
            System.out.println( val );
        }
        /**
         *
         * new FlatMapFunction<String, String>两个string分别代表输入和输出类型
         * Override的call方法需要自己实现一个转换的方法，并返回一个Iterable的结构
         *
         * flatmap属于一类非常常用的spark函数，简单的说作用就是将一条rdd数据使用你定义的函数给分解成多条rdd数据
         * 例如，当前状态下，lines这个rdd类型的变量中，每一条数据都是一行String，我们现在想把他拆分成1个个的词的话，
         * 可以这样写 ：
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                String[] words=s.split(" ");
                return Arrays.asList( words ).iterator();
            }
        });
        /**
         * map 键值对 ，类似于MR的map方法
         * pairFunction<T,K,V>: T:输入类型；K,V：输出键值对
         * 需要重写call方法实现转换
         */
        JavaPairRDD<String, Integer> ones = words.mapToPair( new PairFunction<String, String, Integer>() {
            //@Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        //A two-argument function that takes arguments
        // of type T1 and T2 and returns an R.
        /**
         *  reduceByKey方法，类似于MR的reduce
         *  要求被操作的数据（即下面实例中的ones）是KV键值对形式，该方法会按照key相同的进行聚合，在两两运算
         */
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {  //reduce阶段，key相同的value怎么处理的问题
                return i1 + i2;
            }
        });
        //备注：spark也有reduce方法，输入数据是RDD类型就可以，不需要键值对，
        // reduce方法会对输入进来的所有数据进行两两运算

        /**
         * sort，顾名思义，排序
         */
        JavaPairRDD<String,Integer> sort = counts.sortByKey();
        System.out.println("----------next sort----------------------");

        /**
         * collect方法其实之前已经出现了多次，该方法用于将spark的RDD类型转化为我们熟知的java常见类型
         */
        List<Tuple2<String, Integer>> output = sort.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1 + ": " + tuple._2());
        }
        /**
         * 保存函数，数据输出，spark为结果输出提供了很多接口
         */
        sort.saveAsTextFile("/tmp/spark-tmp/test");


       /*    JavaPairRDD<String, Integer> result = pairs.reduceByKey( new Function2<Integer, Integer, Integer>() {
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
                });*/
        sc.close();
    }
}
