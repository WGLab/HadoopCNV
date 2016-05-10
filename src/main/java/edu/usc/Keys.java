package edu.usc;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.WritableComparable;

/*
 *
 * @author Gary Chen, Ph.D.
 */
public class Keys {
}

class RefRangeAlignmentKey implements
        WritableComparable<RefRangeAlignmentKey> {

    private String refname;
    private int position1;
    private int position2;
    private String bases;

    public RefRangeAlignmentKey() {
    }

    public RefRangeAlignmentKey(String refname, int position1, int position2, String bases) {
        this.refname = refname;
        this.position1 = position1;
        this.position2 = position2;
        this.bases = bases;
    }

    public void setRefName(String refname) {
        this.refname = refname;
    }

    public void setPosition1(int position1) {
        this.position1 = position1;
    }

    public void setPosition2(int position2) {
        this.position2 = position2;
    }

    public void setBase(String bases) {
        this.bases = bases;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(refname);
        out.writeInt(position1);
        out.writeInt(position2);
        out.writeUTF(bases);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        refname = in.readUTF();
        position1 = in.readInt();
        position2 = in.readInt();
        bases = in.readUTF();
    }

    @Override
    public int compareTo(RefRangeAlignmentKey other) {
        int cmp = refname.compareTo(other.refname);
        if (cmp != 0) {
            return cmp;
        }

        if (this.position1 < other.position1) {
            return -1;
        } else if (this.position1 > other.position1) {
            return 1;
        } else if (this.position2 < other.position2) {
            return -1;
        } else if (this.position2 > other.position2) {
            return 1;
        } else {
            return bases.compareTo(other.bases);
        }
    }

    @Override
    public String toString() {
        return refname + "\t" + Integer.toString(position1) + "\t" + Integer.toString(position2) + "\t" + bases;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((refname == null) ? 0 : refname.hashCode());
        result = prime * result + (int) (position1 ^ (position1 >>> 32));
        result = prime * result + (int) (position2 ^ (position2 >>> 32));
        result = prime * result + ((bases == null) ? 0 : bases.hashCode());
        return result;
    }
}

class RefPosBaseKey implements
        WritableComparable<RefPosBaseKey> {

    private String refname;
    private int position;
    private int base;

    public RefPosBaseKey() {
    }

    public RefPosBaseKey(String refname, int position, int base) {
        this.refname = refname;
        this.position = position;
        this.base = base;
    }

    public void setRefName(String refname) {
        this.refname = refname;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void setBase(int base) {
        this.base = base;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(refname);
        out.writeInt(position);
        out.writeInt(base);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        refname = in.readUTF();
        position = in.readInt();
        base = in.readInt();
    }

    @Override
    public int compareTo(RefPosBaseKey other) {
        int cmp = refname.compareTo(other.refname);
        if (cmp != 0) {
            return cmp;
        }

        if (this.position < other.position) {
            return -1;
        } else if (this.position > other.position) {
            return 1;
        } else if (this.base < other.base) {
            return -1;
        } else if (this.base > other.base) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return refname + "\t" + Integer.toString(position) + "\t" + Integer.toString(base);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((refname == null) ? 0 : refname.hashCode());
        result = prime * result + (int) (position ^ (position >>> 32));
        result = prime * result + base;
        return result;
    }
}

/*   @param  refname  The chromosome name
 *   @param  bin      The first base pair position of that key

*/
class RefBinKey implements
        WritableComparable<RefBinKey> {

    private String refname;
    private int bin;

    public RefBinKey() {
    }

    public RefBinKey(String refname, int bin) {
        this.refname = refname;
        this.bin = bin;
    }

    public void setRefName(String refname) {
        this.refname = refname;
    }

    public void setBin(int bin) {
        this.bin = bin;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(refname);
        out.writeInt(bin);
    }

    public String getRefName() {
        return refname;
    }

    public int getBin() {
        return bin;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        refname = in.readUTF();
        bin = in.readInt();
    }

    @Override
    public int compareTo(RefBinKey other) {
        int cmp = refname.compareTo(other.refname);
        if (cmp != 0) {
            return cmp;
        } else if (this.bin < other.bin) {
            return -1;
        } else if (this.bin > other.bin) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return refname + "\t" + Integer.toString(bin);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((refname == null) ? 0 : refname.hashCode());
        result = prime * result + bin;
        return result;
    }
}
