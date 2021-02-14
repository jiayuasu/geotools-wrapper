package org.datasyslab.geotools_wrapper.formatMapper.shapefileParser.shapes;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

public class ShapefileRDDTest
        implements Serializable
{

    /**
     * The sc.
     */
    public static JavaSparkContext sc;

    /**
     * The Input location.
     */
    public static String InputLocation;

    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        SparkConf conf = new SparkConf().setAppName("ShapefileRDDTest").setMaster("local[2]").set("spark.executor.cores", "2");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        //Hard code to a file in resource folder. But you can replace it later in the try-catch field in your hdfs system.
    }

    @AfterClass
    public static void tearDown()
            throws Exception
    {
        sc.stop();
    }

    /**
     * Test if shapeRDD get correct number of shapes from .shp file
     *
     * @throws IOException
     */
    @Test
    public void testLoadShapeFile()
            throws IOException
    {
        // load shape with geotool.shapefile
        InputLocation = ShapefileRDDTest.class.getClassLoader().getResource("shapefiles/polygon").getPath();
        // load shapes with our tool
        SpatialRDD shapefileRDD = ShapefileReader.readToGeometryRDD(sc, InputLocation);
        shapefileRDD.analyze();
        assert (shapefileRDD.rawSpatialRDD.count() == 10000);
    }
}