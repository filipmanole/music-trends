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
import java.util.ArrayList;

import com.indvd00m.ascii.render.Region;
import com.indvd00m.ascii.render.Render;
import com.indvd00m.ascii.render.api.ICanvas;
import com.indvd00m.ascii.render.api.IContextBuilder;
import com.indvd00m.ascii.render.api.IRender;
import com.indvd00m.ascii.render.elements.Label;
import com.indvd00m.ascii.render.elements.PseudoText;
import com.indvd00m.ascii.render.elements.Rectangle;
import com.indvd00m.ascii.render.elements.Table;
import com.indvd00m.ascii.render.elements.plot.Axis;
import com.indvd00m.ascii.render.elements.plot.AxisLabels;
import com.indvd00m.ascii.render.elements.plot.Plot;
import com.indvd00m.ascii.render.elements.plot.api.IPlotPoint;
import com.indvd00m.ascii.render.elements.plot.misc.PlotPoint;

import util.*;

public class Task3 {

    public static int GENRES_COUNT = Genre.values().length;
    public static float[][] crossover = new float[GENRES_COUNT][GENRES_COUNT];

    public static void main(String args[]) throws Exception {

        GenreDetector gd = new GenreDetector();
        
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        ctx.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        ctx.setLogLevel("ERROR");

        JavaRDD<String> songLines = ctx.textFile(args[0], 1);

        JavaPairRDD<Genre, Genre> statistic = songLines.mapToPair((String s) -> {
            Set<SongProperty> reqProperties = new HashSet<SongProperty>();
            reqProperties.add(SongProperty.ARTIST_TERMS);
            reqProperties.add(SongProperty.ARTIST_TERMS_FREQ);

            HashMap<SongProperty, String> result = InputParser.getSongProperties(s, reqProperties);
            String[] artistTerms = InputParser.stringToArrayString(result.get(SongProperty.ARTIST_TERMS));
            float[] artistTermsFreq = InputParser.stringToArrayFloat(result.get(SongProperty.ARTIST_TERMS_FREQ));

            Tuple2<Genre, Genre> topGenre = gd.detectGenre(artistTerms, artistTermsFreq);

            return topGenre;
        });

        for (Genre genre: Genre.values()) {
            int index = genre.ordinal();
            JavaPairRDD<Genre, Integer> g = statistic.filter((t) -> t._1 == genre)
                .mapToPair((t) -> new Tuple2<>(t._2, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

            List<Tuple2<Genre, Integer>> gStat = g.collect();

            for(Tuple2<Genre, Integer> t : gStat) {
                crossover[index][t._1.ordinal()] = t._2;
                crossover[index][index] += t._2;
            }

            float sum = crossover[index][index];

            for(int i = 0; i < GENRES_COUNT; ++i) {
                if(index == i) {
                    crossover[index][i] = 100.0f;
                } else if(crossover[index][i] != 0) {
                    crossover[index][i] = crossover[index][i] * 100 / sum;
                }
            }
        }

        /***********************************************************************/
        /******************************* PLOT **********************************/
        /***********************************************************************/

        IRender render = new Render();
		IContextBuilder builder = render.newBuilder();
		builder.width(80).height(20);
		builder.element(new PseudoText(" TASK 3"));
		ICanvas canvas = render.render(builder.build());
		String s = canvas.getText();
		System.out.println(s);

        render = new Render();
		builder = render.newBuilder();
		builder.width(90).height(40);
        Table table = new Table(GENRES_COUNT + 1, GENRES_COUNT + 1);

        for(int i = 0; i < GENRES_COUNT; ++i) { 
            table.setElement(1, i + 2, new Label(Genre.values()[i].toString()));
            table.setElement(i + 2, 1, new Label(Genre.values()[i].toString()));
        }

        for(int i = 0; i < GENRES_COUNT; ++i) {
            for(int j = 0; j < GENRES_COUNT; ++j) {
                table.setElement(j + 2, i + 2, new Label(String.format ("%.2f", crossover[i][j])));
            }
        }

		builder.element(table);
		canvas = render.render(builder.build());
		s = canvas.getText();
		System.out.println(s);

        ctx.stop();
    }
}
