package pageRank;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Cluster;

public class GraphBuildReducer extends Reducer<Text,Text,Text,Text> {

    private Text outValue = new Text();
    private long totalNodeNum;
    private double initialPageRank;
    private HashSet<String> titleList = new HashSet<String>();

    public void setup(Context context) throws IOException, InterruptedException{

        Configuration conf = context.getConfiguration();
        Cluster cluster = new Cluster(conf);
        Job currentJob = cluster.getJob(context.getJobID());
        totalNodeNum = currentJob.getCounters().findCounter(COUNTER.TOTAL_NODE_NUMBER).getValue();
	    initialPageRank = 1.0/(double)totalNodeNum;

        titleList.clear();
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String title = key.toString();
        StringBuilder builder = new StringBuilder();

        for (Text val: values) {
            String link = val.toString();
            if(title.equals(" ")) {
                //we use this to remove missing link
                titleList.add(link);
            }
            else if(titleList.contains(link)) {
                builder.append("\t" + link);
            }
        }
        if(title.equals(" ")) return;
        else {
            //output format : <key  pr  link1  link2...>
            outValue.set(String.valueOf(initialPageRank) + builder.toString());
            context.write(key, outValue);
        }

    }
}
