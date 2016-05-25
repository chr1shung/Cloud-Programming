package pageRank;

import java.io.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.Counters;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, PageRankValuePair> {

    private Text outKey = new Text();
    private PageRankValuePair outValue = new PageRankValuePair();
    private double deadEndSum;
    private long deadEndCount;

    public void setup(Context context) throws IOException, InterruptedException {

        deadEndSum = 0;
        deadEndCount = 0;
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //title  pr  link1  link2...
        String line = value.toString();
        String info[] = line.split("\\t", 3);
        String title = info[0];
        String pr = info[1];

        //dnagling node
        if(info.length == 2) {
            double originalPageRank = Double.parseDouble(pr);
            deadEndSum += originalPageRank;
            deadEndCount += 1;
            setOutputKeyValue(context, title, "", originalPageRank, false);
        }
        else {
            double originalPageRank = Double.parseDouble(pr);
            String links[] = info[2].split("\\t");
            int outDegree = links.length;
            double newPageRank = originalPageRank/(double)outDegree;
            for(String link: links) {
                setOutputKeyValue(context, link, "", newPageRank, true);
            }
            setOutputKeyValue(context, title, info[2], originalPageRank, false);
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {

        deadEndSum *= 1E15;
        context.getCounter(COUNTER.DANGLING_NODE_PAGE_RANK_SUM).increment((long)deadEndSum);
        context.getCounter(COUNTER.DANGLING_NODE_NUMBER).increment(deadEndCount);
    }

    private void setOutputKeyValue(Context context, String key, String links, double score, boolean isLink)
    throws IOException, InterruptedException {

        outKey.set(key);
        outValue.setLinks(links);
        outValue.setScore(score);
        outValue.setIsLink(isLink);
        context.write(outKey, outValue);
    }
}
