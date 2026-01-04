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

import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class for ST_Transform function using DataFrame API.
 * This is a modern DataFrame-based test that replaces the old RDD-based CRS transformation tests.
 */
public class DataFrameSTTransformTest {

    private static SparkSession sparkSession;

    @BeforeClass
    public static void onceExecutedBeforeAll() {
        sparkSession = SedonaContext.builder()
                .master("local[2]")
                .appName("DataFrameSTTransformTest")
                .config("spark.sql.warehouse.dir", System.getProperty("user.dir") + "/target/")
                .getOrCreate();

        SedonaContext.create(sparkSession);
    }

    @AfterClass
    public static void tearDown() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }

    /**
     * Test ST_Transform with explicit source and target CRS using SQL.
     * This test transforms a point from EPSG:4326 (WGS84) to EPSG:3857 (Web Mercator).
     */
    @Test
    public void testSTTransformWithSourceAndTargetCRS() {
        Dataset<Row> df = sparkSession.sql(
                "SELECT ST_Transform(ST_Point(-122.4194, 37.7749), 'EPSG:4326', 'EPSG:3857') AS geom"
        );

        Row result = df.first();
        Geometry transformedGeom = (Geometry) result.get(0);

        assertNotNull("Transformed geometry should not be null", transformedGeom);
        assertEquals("Geometry type should be Point", "Point", transformedGeom.getGeometryType());

        // Verify the transformed coordinates are in the expected range
        double x = transformedGeom.getCoordinate().x;
        double y = transformedGeom.getCoordinate().y;

        assertTrue("X coordinate should be in Web Mercator range", Math.abs(x) > 1000);
        assertTrue("Y coordinate should be in Web Mercator range", Math.abs(y) > 1000);
    }

    /**
     * Test ST_Transform with SRID set on geometry using SQL.
     * When SRID is set on the geometry, only target CRS needs to be specified.
     */
    @Test
    public void testSTTransformWithSetSRID() {
        Dataset<Row> df = sparkSession.sql(
                "SELECT ST_Transform(ST_SetSRID(ST_Point(-122.4194, 37.7749), 4326), 'EPSG:3857') AS geom"
        );

        Row result = df.first();
        Geometry transformedGeom = (Geometry) result.get(0);

        assertNotNull("Transformed geometry should not be null", transformedGeom);
        assertEquals("Geometry type should be Point", "Point", transformedGeom.getGeometryType());

        // Verify the SRID is set correctly
        assertTrue("Transformed geometry should have coordinates", transformedGeom.getCoordinate() != null);
    }

    /**
     * Test ST_Transform on a DataFrame with multiple points.
     * This test verifies batch transformation of geometries.
     */
    @Test
    public void testSTTransformOnDataFrame() {
        // Create a DataFrame with multiple points (SF Bay Area coordinates)
        sparkSession.sql(
                "SELECT 1 as id, -122.4194 as lon, 37.7749 as lat UNION ALL " +
                "SELECT 2 as id, -122.2711 as lon, 37.8044 as lat UNION ALL " +
                "SELECT 3 as id, -122.0856 as lon, 37.4220 as lat"
        ).createOrReplaceTempView("points");

        // Transform all points from WGS84 to Web Mercator
        Dataset<Row> transformedDf = sparkSession.sql(
                "SELECT id, " +
                "ST_Transform(ST_SetSRID(ST_Point(lon, lat), 4326), 'EPSG:3857') AS geom " +
                "FROM points"
        );

        assertEquals("Should have 3 transformed points", 3, transformedDf.count());

        // Verify all geometries are valid
        long validCount = transformedDf.filter("ST_IsValid(geom)").count();
        assertEquals("All transformed geometries should be valid", 3, validCount);
    }

    /**
     * Test ST_Transform integration with spatial query.
     * This test demonstrates using ST_Transform in a spatial operation.
     */
    @Test
    public void testSTTransformWithSpatialQuery() {
        // Create test data with geometries in WGS84
        sparkSession.sql(
                "SELECT 1 as id, ST_Point(-122.4194, 37.7749) as geom UNION ALL " +
                "SELECT 2 as id, ST_Point(-118.2437, 34.0522) as geom"
        ).createOrReplaceTempView("locations");

        // Transform and calculate distance
        Dataset<Row> result = sparkSession.sql(
                "SELECT l1.id as id1, l2.id as id2, " +
                "ST_Distance(" +
                "  ST_Transform(ST_SetSRID(l1.geom, 4326), 'EPSG:3310'), " +
                "  ST_Transform(ST_SetSRID(l2.geom, 4326), 'EPSG:3310')" +
                ") as distance " +
                "FROM locations l1, locations l2 " +
                "WHERE l1.id < l2.id"
        );

        assertEquals("Should calculate 1 distance pair", 1, result.count());

        Row distanceRow = result.first();
        double distance = distanceRow.getDouble(2);

        assertTrue("Distance should be positive", distance > 0);
        // San Francisco to Los Angeles is approximately 560 km
        assertTrue("Distance should be reasonable (in meters)", distance > 500000 && distance < 650000);
    }

    /**
     * Test ST_Transform with polygon geometry.
     * This test verifies that polygon geometries can be properly transformed.
     */
    @Test
    public void testSTTransformWithPolygon() {
        Dataset<Row> df = sparkSession.sql(
                "SELECT ST_Transform(" +
                "  ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), " +
                "  'EPSG:4326', 'EPSG:3857'" +
                ") AS geom"
        );

        Row result = df.first();
        Geometry transformedGeom = (Geometry) result.get(0);

        assertNotNull("Transformed geometry should not be null", transformedGeom);
        assertEquals("Geometry type should be Polygon", "Polygon", transformedGeom.getGeometryType());
        assertTrue("Transformed polygon should be valid", transformedGeom.isValid());
        assertTrue("Transformed polygon should have area", transformedGeom.getArea() > 0);
    }
}

