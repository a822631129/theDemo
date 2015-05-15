package com.scistor.spark;

import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkwithHcatalog {

	// private static final Pattern SPACE = Pattern.compile(" ");

	@SuppressWarnings({ "serial", "rawtypes" })
	public static void main(String[] args) throws Exception {
		System.out.println("=========这是一个Spark 本地调用hcatalog的算子===========");
		for (int i = 0; i < args.length; i++) {
			System.out.println("=========MAIN中第" + i + "个参数======" + args[i]);
		}

		if (args.length < 2) {
			System.err.println("请填写输入输出路径");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("SparkWordCountDemo");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		// JavaRDD<String> lines = ctx.textFile(args[0], 1);

		Configuration inputConf = new Configuration();
		HCatInputFormat.setInput(inputConf, args[0], args[1]);

		JavaPairRDD<WritableComparable, HCatRecord> records = ctx
				.newAPIHadoopRDD(inputConf, HCatInputFormat.class,
						WritableComparable.class, HCatRecord.class);

		JavaRDD<HCatRecord> onlyRecords = records
				.map(new Function<Tuple2<WritableComparable, HCatRecord>, HCatRecord>() {

					@Override
					public HCatRecord call(
							Tuple2<WritableComparable, HCatRecord> arg0)
							throws Exception {
						return arg0._2;
					}

				});

		JavaPairRDD<Object, Integer> words = onlyRecords
				.mapToPair(new PairFunction<HCatRecord, Object, Integer>() {

					@Override
					public Tuple2<Object, Integer> call(HCatRecord arg0)
							throws Exception {
						return new Tuple2<Object, Integer>(arg0.get(0), 1);
					}

				});

		JavaPairRDD<Object, Integer> counts = words
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer arg0, Integer arg1)
							throws Exception {
						return arg0 + arg1;
					}

				});
		// for ( Tuple2<Object, Integer> count : counts.collect()) {
		// ;
		// }

		List<Tuple2<Object, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(args[2]), conf);
		final Path outPath = new Path(args[2]);
		if (fileSystem.exists(outPath)) {
			fileSystem.delete(outPath, true);
		}
		counts.saveAsTextFile(args[2]);

		ctx.stop();
	}
}
