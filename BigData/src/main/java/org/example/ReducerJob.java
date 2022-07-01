package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerJob extends Reducer<Text, Text, NullWritable, Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        String[] dataAtlet = key.toString().split(",");
        String nama = dataAtlet[0];
        String jenisKelamin = dataAtlet[1];
        String negaraAsal = dataAtlet[2];
        String jenisOlahraga = dataAtlet[3];

        int medaliEmas = 0;
        int medaliPerak = 0;
        int medaliPerunggu = 0;

        for(Text value : values)
        {
            String[] medals = value.toString().split("#");

            medaliEmas += Integer.parseInt(medals[0]);
            medaliPerak += Integer.parseInt(medals[1]);
            medaliPerunggu += Integer.parseInt(medals[2]);
        }

        context.write(NullWritable.get(), new Text(nama + "," + jenisKelamin + "," + "," + negaraAsal + "," + jenisOlahraga + "," + String.valueOf(medaliEmas) + "," + String.valueOf(medaliPerak) + "," + String.valueOf(medaliPerunggu)));
    }
}
