package pageRank;

import java.io.*;
import java.lang.Character;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.Counters;

public class GraphBuildMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        Pattern p1 = Pattern.compile("<title>(.+?)</title>");
        Pattern p2 = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");
        Matcher m1 = p1.matcher(line);
        Matcher m2 = p2.matcher(line);
        boolean doHaveLink = false;
	    if(m1.find()) {
            String title = m1.group(1);
            title = convertSpecial(title);
            outKey.set(title);
    	    while(m2.find()) {
                if(!doHaveLink) doHaveLink = true;
                String link = m2.group(1);
                link = convertSpecial(link);
                link = processFirstLetter(link);
                outValue.set(link);
                context.write(outKey, outValue);
            }
            if(!doHaveLink) {
                outValue.set("");
                context.write(outKey, outValue);
            }

            // we add this to remove missing link
            outKey.set(" ");
            outValue.set(title);
            context.write(outKey, outValue);

            context.getCounter(COUNTER.TOTAL_NODE_NUMBER).increment(1);
        }
    }

    private String convertSpecial(String input) {
        input = input.replaceAll("&lt;", "<");
        input = input.replaceAll("&gt;", ">");
        input = input.replaceAll("&amp;", "&");
        input = input.replaceAll("&quot;", "\"");
        input = input.replaceAll("&apos;", "\'");
        return input;
    }

    private String processFirstLetter(String input) {

        char firstLetter = input.charAt(0);
        if(Character.isLetter(firstLetter)) {
			if (input.length() == 1){
				return input.toUpperCase();
			}
			else{
				return input.substring(0, 1).toUpperCase() + input.substring(1);
			}
		}
		else{
			return input;
		}
    }
}
