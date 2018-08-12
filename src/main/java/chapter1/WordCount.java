package chapter1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created with IDEA
 * Author:catHome
 * Description: 实现mr的典型案例——wordcount词频统计功能
 * Time:Create on 2018/8/11 9:49
 */
public class WordCount {

    /**
     * mapper class
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text mapOutputKey = new Text();

        private static final IntWritable MAP_OUT_VALUE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //文本行数据（字符串）
            String lineValue = value.toString();
            //StringTokenizer切割字符串
            StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
            while (stringTokenizer.hasMoreTokens()){
                String wordValue = stringTokenizer.nextToken();
                mapOutputKey.set(wordValue);
                context.write(mapOutputKey,MAP_OUT_VALUE);
            }
        }
    }

    /**
     * reduce class,map的输出类型就是reduce的输入类型
     */
    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable reduceOutputValue = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /**
             * 临时变量
             */
            int sum = 0;
            for (IntWritable value:values){
                sum+=value.get();
            }
            reduceOutputValue.set(sum);
            context.write(key,reduceOutputValue);
        }
    }

    /**
     * 组装job
     */
    public int run(String[] args) throws Exception {
        //1、get configuration
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.17.18:8020");
        configuration.set("dfs.permissions", "false");
        //2.get job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        //3、set job(input->map->reduce->output)
        //3.1 input
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);
        //3.2 map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //3.3 reduce
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //3.4 output
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);
        //4、submit job,verbose为true能查看jobhistory信息
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int status = new WordCount().run(args);
        //System.exit(0)是正常退出程序，而System.exit(1)或者说非0表示非正常退出程序
        System.exit(status);
    }

}
