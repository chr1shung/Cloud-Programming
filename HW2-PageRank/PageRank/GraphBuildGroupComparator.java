package pageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GraphBuildGroupComparator extends WritableComparator {

    public GraphBuildGroupComparator() {
        super(Text.class, true);
    }


    public int compare(WritableComparable o1, WritableComparable o2) {
        Text key1 = (Text) o1;
        Text key2 = (Text) o2;

        return key1.compareTo(key2);
    }
}
