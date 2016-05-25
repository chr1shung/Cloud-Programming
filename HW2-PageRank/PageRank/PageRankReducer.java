package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Cluster;

public class PageRankReducer extends Reducer<Text,PageRankValuePair,Text,Text> {

    private Text outKey = new Text();
    private PageRankValuePair outValue = new PageRankValuePair();
    private double danglingNodesPageRankSum;
    private double totalNodeNum;
    private double errorSum;
    static final double alpha = 0.85;

    public void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        Cluster cluster = new Cluster(conf);
        Job currentJob = cluster.getJob(context.getJobID());
        long temp = currentJob.getCounters().findCounter(COUNTER.DANGLING_NODE_PAGE_RANK_SUM).getValue();
        danglingNodesPageRankSum = (double)temp/1E15;

        totalNodeNum = conf.getDouble("totalNodeNumber", 0.0);

        errorSum = 0;
    }

    public void reduce(Text key, Iterable<PageRankValuePair> values, Context context) throws IOException, InterruptedException {

        double previousPageRank = 0.0;
        double partialPageRankSum = 0.0;

        for(PageRankValuePair val: values) {
            if(val.isLink()) {
                partialPageRankSum += val.getScore();
            }
            else {
                previousPageRank = val.getScore();
                outValue.setLinks(val.getLinks());
            }
        }

        double newPageRank = (1.0 - alpha)*(1.0/totalNodeNum) + alpha*(partialPageRankSum + danglingNodesPageRankSum/totalNodeNum);
        errorSum += Math.abs(newPageRank - previousPageRank);
        outValue.setScore(newPageRank);

        context.write(key, new Text(outValue.toString()));
    }

    public void cleanup(Context context) throws IOException, InterruptedException {

        errorSum *= 1E15;
        context.getCounter(COUNTER.ERROR).increment((long)errorSum);
    }
}
