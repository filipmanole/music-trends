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
import com.indvd00m.ascii.render.elements.PseudoText;
import com.indvd00m.ascii.render.elements.Rectangle;
import com.indvd00m.ascii.render.elements.plot.Axis;
import com.indvd00m.ascii.render.elements.plot.AxisLabels;
import com.indvd00m.ascii.render.elements.plot.Plot;
import com.indvd00m.ascii.render.elements.plot.api.IPlotPoint;
import com.indvd00m.ascii.render.elements.plot.misc.PlotPoint;

import util.*;

public class Task1 {
    public static void main(String args[]) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        ctx.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        ctx.setLogLevel("ERROR");

        JavaRDD<String> songLines = ctx.textFile(args[0], 1);

        JavaPairRDD<Integer, Tuple2<Float, Integer>> records = songLines.mapToPair((String s) -> {
            Set<SongProperty> reqProperties = new HashSet<SongProperty>();
            reqProperties.add(SongProperty.YEAR);
            reqProperties.add(SongProperty.LOUDNESS);

            HashMap<SongProperty, String> result = InputParser.getSongProperties(s, reqProperties);
            Integer year = Integer.parseInt(result.get(SongProperty.YEAR));
            Float loudness = Float.parseFloat(result.get(SongProperty.LOUDNESS));

            return new Tuple2<>(year, new Tuple2<>(loudness, 1));
        });

        JavaPairRDD<Integer, Tuple2<Float, Integer>> prepareForAverage = records.reduceByKey((t1, t2) -> {
            return new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2);
        });

        JavaPairRDD<Integer, Float> statistic = prepareForAverage.mapToPair((t) -> {
            return new Tuple2<>(t._1, t._2._1 / t._2._2);
        });

        JavaPairRDD<Integer, Float> filteredStat = statistic.filter((t) -> t._1 != 0);

        JavaPairRDD<Integer, Float> sortedStat = filteredStat.sortByKey();

        List<Tuple2<Integer, Float>> output = sortedStat.collect();


        /***********************************************************************/
        /******************************* PLOT **********************************/                    
        /***********************************************************************/

        IRender render = new Render();
		IContextBuilder builder = render.newBuilder();
		builder.width(80).height(20);
		builder.element(new PseudoText(" TASK 1"));
		ICanvas canvas = render.render(builder.build());
		String s = canvas.getText();
		System.out.println(s);

		List<IPlotPoint> points = new ArrayList<IPlotPoint>();
		for (Tuple2<Integer, Float> tuple : output) {
			IPlotPoint plotPoint = new PlotPoint(tuple._1, tuple._2);
			points.add(plotPoint);
        }

		IRender renderPlot = new Render();
		IContextBuilder builderPlot = renderPlot.newBuilder();
		builderPlot.width(100).height(33);
		builderPlot.element(new Rectangle(0, 0, 100, 31));
		builderPlot.layer(new Region(1, 1, 98, 31));
		builderPlot.element(new Axis(points, new Region(0, 0, 98, 31)));
		builderPlot.element(new AxisLabels(points, new Region(0, 0, 98, 31)));
		builderPlot.element(new Plot(points, new Region(0, 0, 98, 31)));
		ICanvas canvasPlot = renderPlot.render(builderPlot.build());
		String stringPlot = canvasPlot.getText();
		System.out.println(stringPlot);

        ctx.stop();
    }
}
