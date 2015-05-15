package com.scistor.spark;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import test.third.jar.Third;

public final class SparkwithDepRemote {

	private static final Pattern SPACE = Pattern.compile(" ");

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		System.out.println("=========这是一个Spark 远程调用third的算子===========");
		for (int i = 0; i < args.length; i++) {
			System.out.println("=========MAIN中第" + i + "个参数======" + args[i]);
		}

		if (args.length < 2) {
			System.err.println("请填写输入输出路径");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("SparkWordCountDemoRemoteDep");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
		// JavaRDD<String> lines =
		// ctx.textFile("hdfs://192.168.8.101/test/words", 1);

		JavaRDD<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterable<String> call(String s) {
						return Arrays.asList(SPACE.split(s));
					}
				});

		JavaPairRDD<String, Integer> ones = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String s) {

						return new Tuple2<String, Integer>(s, 1);
					}
				});

		JavaPairRDD<String, Integer> counts = ones
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer i1, Integer i2) {

						Third third = new Third();// //////////////////
						third.id = 2014;
						third.name = "scistorscistorscistorscistor";
						System.out.println("==========" + third.hello());

						return i1 + i2;
					}
				});

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		// System.out.println("传入的输出路径为====" + args[1]);

		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(args[1]), conf);
		final Path outPath = new Path(args[1]);
		if (fileSystem.exists(outPath)) {
			fileSystem.delete(outPath, true);
		}
		counts.saveAsTextFile(args[1]);

		ctx.stop();
	}
}
