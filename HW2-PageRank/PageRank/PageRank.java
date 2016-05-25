package pageRank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.Counters;

enum COUNTER {
    TOTAL_NODE_NUMBER,
    DANGLING_NODE_PAGE_RANK_SUM,
    DANGLING_NODE_NUMBER,
    ERROR;
}

public class PageRank {

    public static void main(String[] args) throws Exception {


        long totalNodeNumber = graphBuild(args[0], "p1");
    	System.out.println("=====GraphBuild Success!=====");
    	System.out.println("===== Total Node Number : " + totalNodeNumber + " =====");
        String in = "p1";
        String out = "p2";
        ArrayList<Double> errList = new ArrayList<Double>();

        int iter = 1;
        while(true) {
            long currentIterateError = calculatePageRank(in, out, totalNodeNumber);
            System.out.println("=====iter " + iter++ + " end!=====");
            System.out.println("=====error this iter :" + (double)currentIterateError/1E15 + " =====");
            errList.add((double)currentIterateError/1E15);
            if((double)currentIterateError/1E15 < 0.001) {
                //convergence is assumed
                break;
            }
            String tp = in;
            in = out;
            out = tp;
        }
        //System.out.println(out);

        sortResult(out, args[1]);

        for(int i = 0; i < errList.size(); i++) {
            System.out.println("iter " + (i+1) + " error = " + errList.get(i));
        }
    }

    private static long graphBuild(String inputPath, String outputPath)
    throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Build Graph");
        job.setJarByClass(PageRank.class);

        // set the class of each stage in mapreduce
        job.setMapperClass(GraphBuildMapper.class);
	    job.setGroupingComparatorClass(GraphBuildGroupComparator.class);
        job.setReducerClass(GraphBuildReducer.class);

        // set the output class of Mapper and Reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set the number of reducer
        job.setNumReduceTasks(1);

        // add input/output path
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        return job.getCounters().findCounter(COUNTER.TOTAL_NODE_NUMBER).getValue();
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static long calculatePageRank(String inputPath, String outputPath, long totalNodeNumber)
    throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.setDouble("totalNodeNumber", (double)totalNodeNumber);

        Job job = Job.getInstance(conf, "Page Rank");
        job.setJarByClass(PageRank.class);

        // set the class of each stage in mapreduce
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        // set the output class of Mapper and Reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageRankValuePair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set the number of reducer
        job.setNumReduceTasks(1);

        // add input/output path
        try {
            clearOutputDirectory(conf, outputPath);
        } catch (Exception e) {}
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        return job.getCounters().findCounter(COUNTER.ERROR).getValue();
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void sortResult(String inputPath, String outputPath)
    throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Sort Result");
        job.setJarByClass(PageRank.class);

        // set the class of each stage in mapreduce
        job.setMapperClass(SortResultMapper.class);
        job.setSortComparatorClass(SortResultKeyComparator.class);
        job.setReducerClass(SortResultReducer.class);

        // set the output class of Mapper and Reducer
        job.setMapOutputKeyClass(SortResultKeyPair.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // set the number of reducer
        job.setNumReduceTasks(1);

        // add input/output path
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void clearOutputDirectory(Configuration conf, String outputPath) throws Exception{
		FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(outputPath);
		if (fileSystem.exists(path)){
			fileSystem.delete(path, true);
		}
		fileSystem.close();
	}
}
