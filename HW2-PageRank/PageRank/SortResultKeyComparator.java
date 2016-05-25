package pageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortResultKeyComparator extends WritableComparator {

	public SortResultKeyComparator() {
		super(SortResultKeyPair.class, true);
	}


	public int compare(WritableComparable o1, WritableComparable o2) {
		SortResultKeyPair key1 = (SortResultKeyPair) o1;
		SortResultKeyPair key2 = (SortResultKeyPair) o2;

		double result =  key1.getScore() - key2.getScore();
		if(result == 0) {
			result = key1.getTitle().compareTo(key2.getTitle());
		}
		else if(result > 0) result = -1;
		else if(result < 0) result = 1;
		
		return (int)result;
	}
}
