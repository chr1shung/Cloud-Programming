package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class SortResultReducer extends Reducer<SortResultKeyPair,NullWritable,Text,DoubleWritable> {

    private Text outKey = new Text();
    private DoubleWritable outValue = new DoubleWritable();

    public void reduce(SortResultKeyPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        outKey.set(key.getTitle());
        outValue.set(key.getScore());
        context.write(outKey, outValue);
    }
}
