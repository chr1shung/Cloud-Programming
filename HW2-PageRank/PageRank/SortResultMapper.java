package pageRank;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class SortResultMapper extends Mapper<LongWritable, Text, SortResultKeyPair, NullWritable> {

    private SortResultKeyPair outKey = new SortResultKeyPair();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //title  pr  link1  link2...
        String line = value.toString();
        String info[] = line.split("\\t", 3);
        String title = info[0];
        String pr = info[1];
        double pageRank = Double.parseDouble(pr);

        outKey.setTitle(title);
        outKey.setScore(pageRank);
        context.write(outKey, NullWritable.get());
    }
}
