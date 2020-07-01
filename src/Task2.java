import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;

import com.indvd00m.ascii.render.Region;
import com.indvd00m.ascii.render.Render;
import com.indvd00m.ascii.render.api.ICanvas;
import com.indvd00m.ascii.render.api.IContextBuilder;
import com.indvd00m.ascii.render.api.IRender;
import com.indvd00m.ascii.render.elements.PseudoText;
import com.indvd00m.ascii.render.elements.Rectangle;
import com.indvd00m.ascii.render.elements.plot.Axis;
import com.indvd00m.ascii.render.elements.plot.AxisLabels;
import com.indvd00m.ascii.render.elements.plot.Plot;
import com.indvd00m.ascii.render.elements.plot.api.IPlotPoint;
import com.indvd00m.ascii.render.elements.plot.misc.PlotPoint;

import util.*;

public class Task2 {

    /* Rithm Indicators */
    public static Integer LARGISIMO = 20;
    public static Integer GRAVE = 40;
    public static Integer LENTO = 60;
    public static Integer LARGHETTO = 66;
    public static Integer ADAGIO = 76;
    public static Integer ADAIGIETTO = 80;
    public static Integer ANDANTE = 108;
    public static Integer MODERATO = 110;
    public static Integer ALLEGRETTO = 112;
    public static Integer ALLEGRO_MODERATO = 124;
    public static Integer ALLEGRO = 139;
    public static Integer ALLEGRISSIMO = 168;
    public static Integer PRESTO = 200;
    public static Integer PRESTISSIMO = 270;

    public static Integer findRithm(Integer r) {
        if (r < LARGISIMO)
            return LARGISIMO;
        if (r < GRAVE)
            return GRAVE;
        if (r < LENTO)
            return LENTO;
        if (r < LARGHETTO)
            return LARGHETTO;
        if (r < ADAGIO)
            return ADAGIO;
        if (r < ADAIGIETTO)
            return ADAIGIETTO;
        if (r < ANDANTE)
            return ANDANTE;
        if (r < MODERATO)
            return MODERATO;
        if (r < ALLEGRO_MODERATO)
            return ALLEGRO_MODERATO;
        if (r < ALLEGRO)
            return ALLEGRO;
        if (r < ALLEGRISSIMO)
            return ALLEGRISSIMO;
        if (r < PRESTO)
            return PRESTO;

        return PRESTISSIMO;
    }

    public static void plot(List<Tuple2<Integer, Integer>> list) {
        List<IPlotPoint> points = new ArrayList<IPlotPoint>();
        for (Tuple2<Integer, Integer> tuple : list) {
            IPlotPoint plotPoint = new PlotPoint(tuple._1, tuple._2);
            points.add(plotPoint);
        }

        IRender renderPlot = new Render();
        IContextBuilder builderPlot = renderPlot.newBuilder();
        builderPlot.width(100).height(50);
        builderPlot.element(new Rectangle(0, 0, 100, 50));
        builderPlot.layer(new Region(1, 1, 98, 48));
        builderPlot.element(new Axis(points, new Region(0, 0, 98, 48)));
        builderPlot.element(new AxisLabels(points, new Region(0, 0, 98, 48)));
        builderPlot.element(new Plot(points, new Region(0, 0, 98, 48)));
        ICanvas canvasPlot = renderPlot.render(builderPlot.build());
        String stringPlot = canvasPlot.getText();
        System.out.println(stringPlot);
    }

    public static void main(String args[]) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        ctx.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        ctx.setLogLevel("ERROR");

        JavaRDD<String> songLines = ctx.textFile(args[0], 1);

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> statistic = songLines.mapToPair((String s) -> {
            Set<SongProperty> reqProperties = new HashSet<SongProperty>();
            reqProperties.add(SongProperty.MODE);
            reqProperties.add(SongProperty.LOUDNESS);
            reqProperties.add(SongProperty.TEMPO);

            HashMap<SongProperty, String> result = InputParser.getSongProperties(s, reqProperties);
            Integer mode = Integer.parseInt(result.get(SongProperty.MODE));
            Integer loudness = Math.round(Float.parseFloat(result.get(SongProperty.LOUDNESS)));
            Integer tempo = Math.round(Float.parseFloat(result.get(SongProperty.TEMPO)));

            return new Tuple2<>(mode, new Tuple2<>(loudness, tempo));
        });

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> minorStat = statistic.filter((t) -> t._1 == 0);
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> majorStat = statistic.filter((t) -> t._1 == 1);

        /* Minor vs Major Loudness */
        JavaPairRDD<Integer, Integer> minorLoudness = minorStat.mapToPair((m) -> new Tuple2<>(m._2._1, 1))
                .reduceByKey((i1, i2) -> i1 + i2).sortByKey();
        JavaPairRDD<Integer, Integer> majorLoudness = majorStat.mapToPair((m) -> new Tuple2<>(m._2._1, 1))
                .reduceByKey((i1, i2) -> i1 + i2).sortByKey();

        List<Tuple2<Integer, Integer>> outputMinorL = minorLoudness.collect();
        List<Tuple2<Integer, Integer>> outputMajorL = majorLoudness.collect();

        /* Minor vs Major Tempo */
        JavaPairRDD<Integer, Integer> minorTempo = minorStat
                .mapToPair((m) -> new Tuple2<>(Task2.findRithm(m._2._2), 1));
        JavaPairRDD<Integer, Integer> majorTempo = majorStat
                .mapToPair((m) -> new Tuple2<>(Task2.findRithm(m._2._2), 1));

        JavaPairRDD<Integer, Integer> minorTempoReduced = minorTempo.reduceByKey((i1, i2) -> i1 + i2);
        JavaPairRDD<Integer, Integer> majorTempoReduced = majorTempo.reduceByKey((i1, i2) -> i1 + i2);

        JavaPairRDD<Integer, Integer> minorTempoReducedSorted = minorTempoReduced.sortByKey();
        JavaPairRDD<Integer, Integer> majorTempoReducedSorted = majorTempoReduced.sortByKey();

        List<Tuple2<Integer, Integer>> outputMinorT = minorTempoReducedSorted.collect();

        List<Tuple2<Integer, Integer>> outputMajorT = majorTempoReducedSorted.collect();

        /***********************************************************************/
        /******************************* PLOT **********************************/
        /***********************************************************************/

        IRender render = new Render();
        IContextBuilder builder = render.newBuilder();
        builder.width(80).height(20);
        builder.element(new PseudoText(" TASK 2"));
        ICanvas canvas = render.render(builder.build());
        String s = canvas.getText();
        System.out.println(s);

        System.out.println("Loudness in functie de numarul de melodii minore");
        Task2.plot(outputMinorT);
        System.out.println("Loudness in functie de numarul de melodii majore");
        Task2.plot(outputMajorT);
        System.out.println("Tempo in functie de numarul de melodii minore");
        Task2.plot(outputMinorL);
        System.out.println("Tempo in functie de numarul de melodii majore");
        Task2.plot(outputMajorL);

        BufferedWriter writer = new BufferedWriter(new FileWriter("MinorTempo"));
        for(Tuple2<Integer, Integer> t : outputMinorT) {
            writer.write("" + t._1 + " " + t._2 + "\n");
        }
        writer.close();

        writer = new BufferedWriter(new FileWriter("MajorTempo"));
        for(Tuple2<Integer, Integer> t : outputMajorT) {
            writer.write("" + t._1 + " " + t._2 + "\n");
        }
        writer.close();

        writer = new BufferedWriter(new FileWriter("MinorLoudness"));
        for(Tuple2<Integer, Integer> t : outputMinorL) {
            writer.write("" + t._1 + " " + t._2 + "\n");
        }
        writer.close();

        writer = new BufferedWriter(new FileWriter("MajorLoudness"));
        for(Tuple2<Integer, Integer> t : outputMajorL) {
            writer.write("" + t._1 + " " + t._2 + "\n");
        }
        writer.close();

        ctx.stop();
    }
}
