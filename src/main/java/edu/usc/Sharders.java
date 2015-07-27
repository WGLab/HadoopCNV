package edu.usc;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class Sharders {
}

class SortRecordReader extends RecordReader<RefPosBaseKey, Text> {

    /* methods are delegated to this variable
     *
     */
    private final RecordReader<LongWritable, Text> reader;

    private final RefPosBaseKey currentKey = new RefPosBaseKey();
    private final Text currentValue = new Text();

    public SortRecordReader(final RecordReader<LongWritable, Text> reader)
            throws IOException {
        this.reader = reader;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public RefPosBaseKey getCurrentKey() {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        reader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException {
        boolean result = reader.nextKeyValue();
        if (!result) {
            return false;
        }
        Text lineRecordReaderValue = reader.getCurrentValue();

        extractKey(lineRecordReaderValue);
        currentValue.set(extractValue(lineRecordReaderValue));
        return true;
    }

    private void extractKey(final Text value)
            throws IOException {
        String[] parts = value.toString().split("\t");
        currentKey.setRefName(parts[0]);
        currentKey.setPosition(Integer.parseInt(parts[1]));
        currentKey.setBase(Integer.parseInt(parts[2]));
        //return newKey;
    }

    private String extractValue(final Text value)
            throws IOException {
        String[] parts = value.toString().split("\t");
        return parts[3];
    }
}

class SortInputFormat
        extends InputFormat {

    private final TextInputFormat inputFormat = new TextInputFormat();

    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException, InterruptedException {
        return inputFormat.getSplits(context);
    }

    @Override
    public RecordReader<RefPosBaseKey, Text> createRecordReader(final InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        context.setStatus(genericSplit.toString());
        return new SortRecordReader(inputFormat.createRecordReader(genericSplit, context));
    }
}
