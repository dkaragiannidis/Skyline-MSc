package datasetgenerator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Generator {

    public static void uniform(int n, int dimensions, String output) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(output));

        double[] x = new double[dimensions];
        for (int i = 0; i < n; i++) {
            for (int d = 0; d < dimensions; d++) {
                x[d] = randomEqual(0, 1);
                bw.write(x[d] + " ");
            }
            bw.write("\n");
        }

        bw.close();
    }

    public static void correlated(int n, int dimensions, String output) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(output));

        double[] x = new double[dimensions];
        for (int i = 0; i < n; i++) {
            double v = randomPeak(0, 1, dimensions);
            for (int d = 0; d < dimensions; d++) {
                x[d] = v;
            }
            double l = v <= 0.5 ? v : 1.0 - v;
            for (int d = 0; d < dimensions; d++) {
                double h = randomNormal(0, l);
                x[d] += h;
                x[(d + 1) % dimensions] -= h;
            }

            boolean flag = false;
            for (int d = 0; d < dimensions; d++) {
                if (x[d] < 0 || x[d] >= 1) {
                    flag = true;
                    break;
                }
            }
            if (flag) {
                i--;
                continue;
            }

            for (int d = 0; d < dimensions; d++) {
                bw.write(x[d] + " ");
            }
            bw.write("\n");
        }
        bw.close();
    }

    public static void anticorrelated(int n, int dimensions, String output) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(output));

        double[] x = new double[dimensions];
        for (int i = 0; i < n; i++) {
            double v = randomNormal(0.5, 0.25);
            for (int d = 0; d < dimensions; d++) {
                x[d] = v;
            }
            double l = v <= 0.5 ? v : 1.0 - v;
            for (int d = 0; d < dimensions; d++) {
                double h = randomEqual(-l, l);
                x[d] += h;
                x[(d + 1) % dimensions] -= h;
            }

            boolean flag = false;
            for (int d = 0; d < dimensions; d++) {
                if (x[d] < 0 || x[d] >= 1) {
                    flag = true;
                    break;
                }
            }
            if (flag) {
                i--;
                continue;
            }

            for (int d = 0; d < dimensions; d++) {
                bw.write(x[d] + " ");
            }
            bw.write("\n");
        }
        bw.close();
    }

    public static void gauss(int n, int dimensions, String output) throws IOException {

        BufferedWriter bw = new BufferedWriter(new FileWriter(output));

        double[] x = new double[dimensions];
        for (int i = 0; i < n; i++) {
            for (int d = 0; d < dimensions; d++) {
                Random rand = new Random();
                x[d] = rand.nextGaussian() * 0.125 + 0.5;
                if (x[d] < 0 || x[d] >= 1) {
                    d--;
                    continue;
                }
                bw.write(x[d] + " ");
            }
            bw.write("\n");
        }
        bw.close();
    }



    
    private static double randomEqual(double min, double max) {
        Random rand = new Random();
        return rand.nextDouble() * (max - min) + min;
    }

    private static double randomPeak(double min, double max, int dim) {
        double sum = 0.0;
        for (int d = 0; d < dim; d++) {
            sum += randomEqual(0, 1);
        }
        sum /= dim;
        return sum * (max - min) + min;
    }

    private static double randomNormal(double med, double var) {
        return randomPeak(med - var, med + var, 12);
    }
}
