package org.example;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperJob extends Mapper<Object, Text, Text, Text>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        try
        {
            if(value.toString().contains("nationality"))
                return;
            else
            {
                String data = value.toString();
                String[] columns = data.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                String nama = columns[1];
                String jenisKelamin = columns[3];
                String negaraAsal = columns[2];
                String jenisOlahraga = columns[7];

                String medaliEmas = columns[8];
                String medaliPerak = columns[9];
                String medaliPerunggu = columns[10];

                context.write(new Text(nama + "," + jenisKelamin + "," + negaraAsal + "," + jenisOlahraga), new Text(medaliEmas + "#" + medaliPerak + "#" + medaliPerunggu));
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
