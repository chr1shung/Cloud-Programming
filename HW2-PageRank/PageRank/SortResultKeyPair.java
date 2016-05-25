package pageRank;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;


public class SortResultKeyPair implements WritableComparable<SortResultKeyPair> {

    private Text title;
	private double score;

	public SortResultKeyPair(){
		score = 0;
		title = new Text();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(score);
		title.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		score = in.readDouble();
		title.readFields(in);
	}

    @Override
    public int compareTo(SortResultKeyPair o) {
        return title.compareTo(o.title);
    }

	public void setScore(double score) {
		this.score = score;
	}

	public void setTitle(String title) {
		this.title.set(title);
	}

	public double getScore() {
		return this.score;
	}

	public String getTitle() {
		return this.title.toString();
	}
}
