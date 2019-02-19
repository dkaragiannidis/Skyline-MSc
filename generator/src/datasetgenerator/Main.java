package datasetgenerator;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {

        final int n =1000000;
        final int d =5;
        final String outputFile = "data3.csv";
//        final int n = Integer.parseInt(args[0]);
//        final int d = Integer.parseInt(args[1]);
//        final String outputFile = args[2];
        final int distribution = 1;

        switch (distribution) {
            case 0:
                Generator.uniform(n, d, outputFile);
                break;
            case 1:
                Generator.correlated(n, d, outputFile);
                break;
            case 2:
                Generator.anticorrelated(n, d, outputFile);
                break;
            case 3:
                Generator.gauss(n, d, outputFile);
                break;
        }
    }
}
