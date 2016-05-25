package pageRank;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;


public class PageRankValuePair implements Writable {

	private double score;
	private Text links;
	private boolean isLink;

	public PageRankValuePair(){
		score = 0;
		links = new Text();
        isLink = false;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(score);
		links.write(out);
        out.writeBoolean(isLink);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		score = in.readDouble();
		links.readFields(in);
        isLink = in.readBoolean();
	}

	public void setScore(double score) {
		this.score = score;
	}

	public void setLinks(String links) {
		this.links.set(links);
	}

    public void setIsLink(boolean isLink) {
        this.isLink = isLink;
    }

	public double getScore() {
		return this.score;
	}

	public String getLinks() {
		return this.links.toString();
	}

    public boolean isLink() {
        return this.isLink;
    }

	public String toString(){
		if ("".equals(links.toString())){
			return String.valueOf(score);
		}
		else{
			return String.valueOf(score)+ "\t" + links.toString();
		}
	}

}
