package chapter1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created with IDEA
 * Author:catHome
 * Description: 优化wordcount词频统计
 * Time:Create on 2018/8/13 8:53
 */
public class OptimizeWordCount extends Configured implements Tool {

    /**
     * mapper class
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text mapOutputKey = new Text();

        private static final IntWritable MAP_OUT_VALUE = new IntWritable(1);


        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

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

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    /**
     * reduce class,map的输出类型就是reduce的输入类型
     */
    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable reduceOutputValue = new IntWritable();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

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

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    /**
     * 组装job
     */
    @Override
    public int run(String[] args) throws Exception {
        //1、get configuration,由于继承Configured
        Configuration configuration = getConf();
        //2.get job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        //3、set job(input->map->reduce->output)
        //3.1 input
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);
        //3.2 map
        job.setMapperClass(WordCount.WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /*shuffle*/
        //shuffle partition分区
//        job.setPartitionerClass(cls);
        // shuffle sort
//        job.setSortComparatorClass(cls);
        //shuffle combainer
//        job.setCombinerClass(cls);
        //shuffle group分组
//        job.setGroupingComparatorClass(cls);

        //3.3 reduce
        job.setReducerClass(WordCount.WordCountReduce.class);
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
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.17.18:8020");
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        /*由于map任务的输出需要写到磁盘并通过网络传输到reducer节点，所以如果使用LZO、LZ4或者Snappy这样的快速压缩方式，是可以提升性能的*/
        //Should the job outputs be compressed? 对map输出任务进行压缩，默认为false
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        //reduce任务对象个数配置，默认为1，需要测试评估决定设置的值以优化
        configuration.set("mapreduce.job.reduces","1");
        //压缩方式
        configuration.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
        ToolRunner.run(configuration,new OptimizeWordCount(),args);
        int status = new WordCount().run(args);
        //System.exit(0)是正常退出程序，而System.exit(1)或者说非0表示非正常退出程序
        System.exit(status);
    }
}
