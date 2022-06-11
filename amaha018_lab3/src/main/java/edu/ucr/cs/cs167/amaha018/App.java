package edu.ucr.cs.cs167.amaha018;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.lang.*;

import java.util.function.Function;

/**
 * Print Numbers based on various conditions, abstract it
 */
public class App {

    static class IsEven implements Function<Integer, Boolean> {
        @Override
        public Boolean apply(Integer x) {
            return x % 2 == 0;
        }
    }

    static class IsOdd implements Function<Integer, Boolean> {
        @Override
        public Boolean apply(Integer x) {
            return x % 2 == 1;
        }
    }

    public static void printEvenNumbers(int from, int to){
        System.out.println("Printing numbers in the range [" + from + "," + to + "]");
        // print all even numbers [from, to] on separate line
        for(int i = from; i <= to; i++){
            if(i % 2 == 0){
                System.out.println(i);
            }
        }
    }

    public static void printOddNumbers(int from, int to){
        System.out.println("Printing numbers in the range [" + from + "," + to + "]");
        // print all odd numbers [from, to] on separate line
        for(int i = from; i <= to; i++){
            if(i % 2 == 1){
                System.out.println(i);
            }
        }
    }

    public static void printNumbers(int from, int to, Function<Integer, Boolean> filter){
        System.out.println("Printing numbers in the range [" + from + "," + to + "]");
        // print all even or odd numbers [from, to] on separate line
        // even or odd depends on filter
        for(int i = from; i <= to; i++){
            if(filter.apply(i)){
                System.out.println(i);
            }
        }
    }

    public static Function<Integer, Boolean> combineWithAnd(Function<Integer, Boolean> ... filters) {
        Function<Integer, Boolean> filter = filters[0];
        for(int i = 1; i < filters.length; i++){
            Function<Integer, Boolean> totalFilter = filter;
            int finalI = i;
            filter = x -> totalFilter.apply(x) && filters[finalI].apply(x);
        }
        return filter;
    }

    public static Function<Integer, Boolean> combineWithOr(Function<Integer, Boolean> ... filters) {
        Function<Integer, Boolean> filter = filters[0];
        for(int i = 1; i < filters.length; i++){
            Function<Integer, Boolean> totalFilter = filter;
            int finalI = i;
            filter = x -> totalFilter.apply(x) || filters[finalI].apply(x);
        }
        return filter;
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 3){
            System.err.println("Error: At least three parameters expected, from, to, and odd.");
            System.exit(-1);
        }
        int from = Integer.parseInt(args[0]);
        int to = Integer.parseInt(args[1]);
        String regex = args[2];
        boolean containCaret = regex.contains("^");
        boolean containv = regex.contains("v");
        String[] bases;
        if(containCaret) {
            bases = regex.split("\\^");
        }
        else if(containv)
            bases =  regex.split("v");
        else
            bases = new String[]{regex}; // just the 1 filter

        Function<Integer, Boolean>[] filters = new Function[bases.length];
        for(int i = 0; i < bases.length; i++){
            int currBase = Integer.parseInt(bases[i]);
            Function<Integer, Boolean> divisibleByBase = x -> x % currBase == 0;
            filters[i] = divisibleByBase;
        }

        Function<Integer, Boolean> filter = null;
        if(containCaret)
            filter = combineWithAnd(filters);
        else if(containv)
            filter = combineWithOr(filters);
        else
            filter = filters[0];

        printNumbers(from,to,filter);


        /*
        //lambda expression
        Function<Integer, Boolean> divisibleByTen = x -> x % 10 == 0;
     //   Function<Integer, Boolean> divisibleByBase = x -> x % base == 0;

        //anonymous class
        Function<Integer, Boolean> divisibleByFive = new Function<Integer, Boolean>() {
            @Override
            public Boolean apply(Integer x) {
                return x % 5 == 0;
            }
        };

        Function<Integer,Boolean> filter = null;
        if(base == 1){
            filter = new IsOdd();
        } else if(base == 2){
            filter = new IsEven();
        }
        filter = divisibleByBase;
*/
    }
}