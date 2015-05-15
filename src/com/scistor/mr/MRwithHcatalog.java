package com.scistor.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class MRwithHcatalog extends Configured implements Tool {

	@SuppressWarnings("rawtypes")
	public static class Map extends
			Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable> {

		private Integer age;
		private HCatSchema schema;

		@Override
		protected void map(WritableComparable key, HCatRecord value,
				Context context) throws IOException, InterruptedException {
			// Get our schema from the Job object.
			schema = HCatInputFormat.getTableSchema(context.getConfiguration());
			// Read the user field.
			age = value.getInteger("age", schema);
			context.write(new IntWritable(age), new IntWritable(1));
		}
	}

	@SuppressWarnings("rawtypes")
	public static class Reduce extends
			Reducer<IntWritable, IntWritable, WritableComparable, HCatRecord> {
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> iter = values.iterator();
			while (iter.hasNext()) {
				sum++;
				iter.next().get();
			}
			HCatRecord record = new DefaultHCatRecord(2);
			record.set(0, key.get());
			record.set(1, sum);
			context.write(null, record);
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("=========这是一个调用HCATALOG的MR算子========");
		int exitcode = ToolRunner.run(new Configuration(),
				new MRwithHcatalog(), args);
		System.exit(exitcode);

	}

	@Override
	public int run(String[] arg) throws Exception {
		Configuration conf = this.getConf();
		for (int i = 0; i < arg.length; i++) {
			System.out.println("====RUN方法中第" + i + "个参数是====" + arg[i]);
		}

		// String dbName = "default";
		// String inputTableName = "student";
		// String outputTableName = "student_out";

		String dbName = arg[0];
		String inputTableName = arg[1];
		String outputTableName = arg[2];

		// String dbName = "";
		// String inputTableName = "";
		// String outputTableName = "";
		// if (this.getConf().getStrings("database").length == 1) {
		// System.out.println("===database===="
		// + this.getConf().getStrings("database")[0]);
		// dbName = this.getConf().getStrings("database")[0];
		// }
		//
		// if (this.getConf().getStrings("inputTableName").length == 1) {
		// System.out.println("===inputTableName===="
		// + this.getConf().getStrings("inputTableName")[0]);
		// inputTableName = this.getConf().getStrings("inputTableName")[0];
		// }
		// if (this.getConf().getStrings("outputTableName").length == 1) {
		// System.out.println("===outputTableName===="
		// + this.getConf().getStrings("outputTableName")[0]);
		// outputTableName = this.getConf().getStrings("outputTableName")[0];
		// }

		// conf.get("tmpjars", value);

		Job job = Job.getInstance(conf);
		job.setJobName("MRwithHcatalog");
		// Read the inputTableName table, partition null, initialize the default
		// database.
		HCatInputFormat.setInput(job, dbName, inputTableName);

		// Initialize HCatoutputFormat
		job.setInputFormatClass(HCatInputFormat.class);
		job.setJarByClass(MRwithHcatalog.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(WritableComparable.class);
		job.setOutputValueClass(DefaultHCatRecord.class);

		String inputJobString = job.getConfiguration().get(
				HCatConstants.HCAT_KEY_JOB_INFO);
		job.getConfiguration().set(HCatConstants.HCAT_KEY_JOB_INFO,
				inputJobString);

		// Write into outputTableName table, partition null, initialize the
		// default database.
		OutputJobInfo outputJobInfo = OutputJobInfo.create(dbName,
				outputTableName, null);
		job.getConfiguration().set(HCatConstants.HCAT_KEY_OUTPUT_INFO,
				HCatUtil.serialize(outputJobInfo));
		HCatOutputFormat.setOutput(job, outputJobInfo);

		HCatSchema s = HCatOutputFormat.getTableSchema(job.getConfiguration());
		HCatOutputFormat.setSchema(job, s);

//		System.out
//				.println("INFO: Output scheme explicity set for writing:" + s);
		job.setOutputFormatClass(HCatOutputFormat.class);
//		System.out.println("Output input table 'inputTableName'："
//				+ inputTableName);
//		System.out.println("Output output table 'outputTableName'："
//				+ outputTableName);
		return (job.waitForCompletion(true) ? 0 : 1);
	}

}
