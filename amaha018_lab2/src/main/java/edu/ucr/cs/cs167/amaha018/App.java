package edu.ucr.cs.cs167.amaha018;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * Copy input file to output file using hadoop
 */
public class App {

    public static void main(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Incorrect number of arguments! Expected two arguments.");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Path i_path = new Path(args[0]);
        Path o_path = new Path(args[1]);
        FileSystem i_fs = i_path.getFileSystem(conf);
        FileSystem o_fs = o_path.getFileSystem(conf);

        if( ! i_fs.exists(i_path) ){ // if the file exists, print error
            System.err.printf("Input file '%s' does not exist!\n", i_path);
            System.exit(-1);
        }
        if( o_fs.exists(o_path) ){ // if the file exists, print error
            System.err.printf("Output file '%s' already exists!\n", o_path);
            System.exit(-1);
        }

        FSDataInputStream i_stream = i_fs.open(i_path);
        FSDataOutputStream o_stream = o_fs.create(o_path);

        long s_time = System.nanoTime();
        //read in input file and write to output file
        byte buffer[] = new byte[10000];
        long byte_num = 0;
        int bytes_read = 0;
        while( (bytes_read = i_stream.read(buffer)) != -1){
            o_stream.write(buffer, 0, bytes_read);
            byte_num += bytes_read;
        }
        //close both files
        i_stream.close(); o_stream.close();

        //calculate elapsed time
        long e_time = System.nanoTime() - s_time;

        System.out.printf("Copied %d bytes from '%s' to '%s' in %f seconds\n",
                byte_num, i_path, o_path, e_time * 1E-9);
    }
}