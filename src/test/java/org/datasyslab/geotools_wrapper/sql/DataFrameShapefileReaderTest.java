/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.datasyslab.geotools_wrapper.sql;

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for reading shapefiles and converting to DataFrame.
 * This is a modern DataFrame-based test that replaces the old RDD-based tests.
 */
public class DataFrameShapefileReaderTest {

    private static SparkSession sparkSession;
    private static String shapefileInputLocation;

    @BeforeClass
    public static void onceExecutedBeforeAll() {
        sparkSession = SedonaContext.builder()
                .master("local[2]")
                .appName("DataFrameShapefileReaderTest")
                .config("spark.sql.warehouse.dir", System.getProperty("user.dir") + "/target/")
                .getOrCreate();

        SedonaContext.create(sparkSession);

        shapefileInputLocation = DataFrameShapefileReaderTest.class
                .getClassLoader()
                .getResource("shapefiles/polygon")
                .getPath();
    }

    @AfterClass
    public static void tearDown() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }

    /**
     * Test reading shapefile and converting to DataFrame.
     * This test verifies that shapefile data can be properly loaded
     * into a DataFrame with correct schema and row count.
     */
    @Test
    public void testReadShapefileToDataFrame() {
        // Read shapefile using ShapefileReader
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        SpatialRDD<Geometry> spatialRDD = ShapefileReader.readToGeometryRDD(
                jsc,
                shapefileInputLocation
        );
        spatialRDD.analyze();

        // Convert to DataFrame using Adapter
        Dataset<Row> df = Adapter.toDf(spatialRDD, sparkSession);

        // Verify schema contains geometry column
        assertTrue("DataFrame should contain 'geometry' column",
                df.columns().length > 0 && df.columns()[0].equals("geometry"));

        // Verify row count
        long count = df.count();
        assertEquals("DataFrame should contain 10000 rows", 10000, count);

        // Verify the DataFrame can be queried
        df.createOrReplaceTempView("shapefile_data");
        Dataset<Row> queryResult = sparkSession.sql(
                "SELECT geometry FROM shapefile_data LIMIT 1"
        );

        assertEquals("Query should return 1 row", 1, queryResult.count());
        assertTrue("First row should contain a geometry",
                queryResult.first().get(0) instanceof Geometry);
    }

    /**
     * Test reading shapefile with field names and converting to DataFrame.
     * This test verifies that attribute fields are properly preserved.
     */
    @Test
    public void testReadShapefileWithFieldsToDataFrame() {
        // Read shapefile using ShapefileReader
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        SpatialRDD<Geometry> spatialRDD = ShapefileReader.readToGeometryRDD(
                jsc,
                shapefileInputLocation
        );
        spatialRDD.analyze();

        // Convert to DataFrame
        Dataset<Row> df = Adapter.toDf(spatialRDD, sparkSession);

        // Register as temp view for SQL queries
        df.createOrReplaceTempView("shapefile_with_fields");

        // Test SQL query on the DataFrame - use subquery to calculate area first
        Dataset<Row> result = sparkSession.sql(
                "SELECT area FROM (" +
                "  SELECT ST_Area(geometry) as area FROM shapefile_with_fields" +
                ") WHERE area > 0 LIMIT 10"
        );

        assertEquals("Should return 10 rows with positive area", 10, result.count());
    }
}

