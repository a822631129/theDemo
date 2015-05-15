package com.scistor.so;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MR extends Configured implements Tool {

	public native String mapReduceSayHello();

	public static void main(String[] args) throws Exception {
		System.out.println("====这是一个MR单词计数====");
		int res = ToolRunner.run(new Configuration(), new MR(), args);
		System.exit(res);

	}

	static class MyMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		protected void map(LongWritable k1, Text v1, Context context)
				throws java.io.IOException, InterruptedException {

			System.out.println("现在是在map方法中");
			File directory = new File("");// 设定为当前文件夹
			try {
				System.out.println("标准路径=====" + directory.getCanonicalPath());// 获取标准的路径
				System.out.println("绝对路径=====" + directory.getAbsolutePath());// 获取绝对路径
			} catch (Exception e) {
			}
			
			System.out.println(new MR().mapReduceSayHello());

			final String[] splited = v1.toString().split("\t");
			for (String word : splited) {
				context.write(new Text(word), new LongWritable(1));
			}
		};
	}

	static class MyReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		protected void reduce(Text k2, java.lang.Iterable<LongWritable> v2s,
				Context ctx) throws java.io.IOException, InterruptedException {

			System.out.println("现在是在reduce方法中");
			File directory = new File("");// 设定为当前文件夹
			try {
				System.out.println("标准路径=====" + directory.getCanonicalPath());// 获取标准的路径
				System.out.println("绝对路径=====" + directory.getAbsolutePath());// 获取绝对路径
			} catch (Exception e) {
			}
			
			System.out.println(new MR().mapReduceSayHello());

			long times = 0L;
			for (LongWritable count : v2s) {
				times += count.get();
			}
			ctx.write(k2, new LongWritable(times));
		};
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] arg) throws Exception {

		for (int i = 0; i < arg.length; i++) {
			System.out.println("==========RUN方法中第" + i + "个参数====" + arg[i]);

		}
		Configuration conf = this.getConf();// 实现了tool之后不能再使用new conf

		final FileSystem fileSystem = FileSystem.get(new URI(arg[0]), conf);
		final Path outPath = new Path(arg[1]);
		if (fileSystem.exists(outPath)) {
			fileSystem.delete(outPath, true);
		}
		final Job job = new Job(conf, MR.class.getSimpleName());
		FileInputFormat.setInputPaths(job, arg[0]);
		job.setJarByClass(MR.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileOutputFormat.setOutputPath(job, outPath);
		job.waitForCompletion(true);
		
		return 0;
	}

	
	
	static {
		System.loadLibrary("mapreduce");
	}
}
