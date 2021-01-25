import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount1 {

    public static void main( String[] args ) {
        // 读取hdfs-site.xml，core-site.xml
        Configuration conf = new Configuration();
        // 设置用户可以跨平台提交，否则提交成功但是执行失败
        conf.set("mapreduce.app-submission.cross-platform","true");
        try{
            FileSystem fs=FileSystem.get(conf);
            fs.delete(new Path("/will/data/output"));

            Job job = Job.getInstance(conf,"WordCount V1.0");

            job.setJarByClass(WordCount1.class);
            // 设置需要执行jar的路径，下面根据Maven的Package命令生成的jar路径
            //job.setJar("E:\\IDEA_workspace\\mapreduce-test\\target\\mapreduce-test-1.0-SNAPSHOT.jar");

            job.setMapperClass(WordCountMapper1.class);
            job.setCombinerClass(WordCountReducer1.class);
            job.setReducerClass(WordCountReducer1.class);

            // job 输出key value 类型，mapper和reducer类型相同可用
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // hdfs路径
            FileInputFormat.addInputPath(job, new Path("/will/data/input"));
            FileOutputFormat.setOutputPath(job, new Path("/will/data/output"));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}