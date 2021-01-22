import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取一行
        String line = value.toString();
        // 空格分隔
        StringTokenizer stringTokenizer = new StringTokenizer(line);
        // 循环空格分隔，给每个计数1
        while(stringTokenizer.hasMoreTokens()){
            word.set(stringTokenizer.nextToken());
            context.write(word, one);
        }
    }
}