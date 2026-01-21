/**
 *  CeCILL FREE SOFTWARE LICENSE AGREEMENT
 *
 * Version 2.1 dated 2013-06-21
 *
 * http://www.cecill.info/licences/Licence_CeCILL_V2.1-en.txt
 */
package org.h2spatial;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.h2.engine.Session;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

/**
 * 
 * @author Bocher Erwan, 2006, PHD in geography
 *
 * Note
 *
 * First extension to expand the H2 database with spatial functions from JTS lib.
 *
 * This extension was developed in my personal time after my PHD in geography.
 *
 * It was published in 2006 on the first french community portal for free geomatics software : http://www.projet-sigle.org
 *
 * Projet-SIGLE stands for "Systèmes et Infrastructures Géographiques LibrEs"
 *
 * @contact erwan.bocher@gmail.com
 * @version 1.0
 * @date 13/11/2006
 * @licence CeCILL
 * @see http://www.cecill.info/
 * 
 *
 * This class allows to add spatial functions to H2 database. H2 is the free SQL
 * database under Mozilla Public Licence 1.1. See :
 * http://www.h2database.com/html/frame.html and http://www.mozilla.org/MPL.
 *
 * 
 * 1. Introduction
 * 
 * In some case we need a simple and portable spatial database.
 *
 * eSDI (embedded Spatial Data Infrastructure) is a new concept and solution
 * for building and managing an SDI.Sometimes, especially in small local administrations,
 * 	there is a need for a more flexible and portable tool. This solution already
 *  exists in conjunction with the ArcGIS software Geodatabase. However, its
 *  use is mainly limited to data storage, or else specialised extensions
 *  such as ArcSDE must be purchased.
 *  While the world of free GIS has robust and powerful spatial databases (PostgreSQL, MySQL),
 *  in many situations these tools are too sophisticated for the needs and uses to which they are put.
 *
 * Therefore, we will present a new type of spatial database based on the free Java database H2-Database.
 * Coupled with a GIS as gvSIG, H2Spatial provides a first level to build a portable
 * spatial data infrastructure.
 *
 *
 * 2.Method
 * 
 * JTS spatial algorithms are exposed as SQL functions using user-Defined Functions available
 * in H2 engine.
 * The spatial functions partially cover the OGC specification "Implementation Specification for Geographic information - Simple feature
 * access - Part 2: SQL option"
 *
 * The JTS geometry object is stored in WKB format in a BLOB datatype.
 *
 * A new domain called GEOMETRY is created to define the geometry datatype.
 *
 * e.g CREATE DOMAIN GEOMETRY AS BLOB
 * 
 * 3.Uses
 * 
 * Create a database see https://h2database.com/html/quickstart.html
 *
 * Create the Geometry datatype
 *
 * CREATE DOMAIN GEOMETRY AS BLOB;
 *
 * Create a table
 * 
 * CREATE TABLE mySpatialTable(gid INT primary key, the_geom geometry);
 * 
 * Insert data
 * 
 * INSERT INTO mySpatialTable VALUES(1, GeomFromText('POINT(12,1)','1')
 * 
 *
 * Test buffer function using :
 * 
 * SELECT buffer(the_geom, 20) FROM mySpatialTable;
 * 
 * Display available functions :
 * 
 * SELECT * FROM INFORMATION_SCHEMA.FUNCTION_ALIASES
 * 
 * 
 * 8.Work in progress
 * 
 * Create an independant tool to load gml file into H2 spatial. Curently you
 * can use geoSQLBuilder.
 * 
 * Improve spatial queries using spatial indexes.
 * 
 * Add geometry datatype in H2 database. Geometry in eWKB format.
 *
 * 
 * 9.License
 * 
 * H2Spatial is distributed under CeCILL license see http://www.cecill.info/
 */

public class GeoSpatialFunctions {

	public GeoSpatialFunctions() {
	}

	public static String GeoVersion() {
		return "1.0";
	}

	public static String LastGeoVersion() {
		return "1.0";
	}

	public static byte[] setWKBGeometry(Geometry arg0, int arg1)
			throws IOException {
		Geometry geom = arg0;
		geom.setSRID(arg1);
		WKBWriter wkbWriter = new WKBWriter(3, 2);
		return wkbWriter.write(geom);
	}

	public static String setWKTGeometry(Geometry arg0, int arg1)
			throws IOException {
		String geomString = arg0.toString();
		String geomWithSRID = (new StringBuilder("SRID=")).append(arg1).append(
				";").append(geomString).toString();
		return geomWithSRID;
	}

	public static Geometry getGeometry(byte arg0[])
			throws ClassNotFoundException, ParseException {
		return wkbreader.read(arg0);
	}

	public static byte[] GeomFromText(String arg0, int arg1)
			throws ParseException, IOException {
		Geometry geom = (new WKTReader()).read(arg0);
		return setWKBGeometry(geom, arg1);
	}

	public static void AddGeometryColumn(Session session, String schemaName,
			String tableName, String columnName, int srid, String geomType,
			int geomDimension) throws SQLException {
		Connection conn = session.createConnection(false);
		Statement stat = conn.createStatement();
		stat.execute((new StringBuilder("ALTER TABLE ")).append(tableName)
				.append(" ADD ").append(columnName).append(" geometry;")
				.toString());
		String insertIntoGeometry_columns = "INSERT INTO geometry_columns  VALUES(?,?,?,?,?,?,?);";
		PreparedStatement prep = conn
				.prepareStatement(insertIntoGeometry_columns);
		prep.setString(1, "");
		prep.setString(2, "");
		prep.setString(3, tableName);
		prep.setString(4, columnName);
		prep.setInt(5, srid);
		prep.setString(6, geomType);
		prep.setInt(7, geomDimension);
		prep.execute();
		prep.close();
		stat.close();
	}

	public static String ToString(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.toString();
	}

	public static String AseWKT(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.toText();
	}

	public static double GeoLength(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.getLength();
	}

	public static double Area(byte arg0[]) throws IOException, ParseException,
			ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.getArea();
	}

	public static int NumPoints(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.getNumPoints();
	}

	public static int Dimension(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.getDimension();
	}

	public static String GeometryType(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.getGeometryType();
	}

	public static String AsText(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.toText();
	}

	public static byte[] AsBinary(byte arg0[]) throws IOException,
			ParseException {
		return arg0;
	}

	public static int SRID(byte arg0[]) throws IOException, ParseException,
			ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.getSRID();
	}

	public static boolean IsEmpty(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.isEmpty();
	}

	public static boolean IsSimple(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.isSimple();
	}

	public static byte[] Boundary(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return setWKBGeometry(geom.getBoundary(), geom.getSRID());
	}

	public static byte[] Envelope(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return setWKBGeometry(geom.getEnvelope(), geom.getSRID());
	}

	public static int NumGeometries(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.getNumGeometries();
	}

	public static byte[] GeometryN(byte arg0[], int arg1) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return setWKBGeometry(geom.getGeometryN(arg1), geom.getSRID());
	}

	public static boolean Equals(byte arg0[], byte arg1[]) {
		return arg0.equals(arg1);
	}

	public static boolean Disjoint(byte arg0[], byte arg1[])
			throws IOException, ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		Geometry geom2 = getGeometry(arg1);
		return geom.disjoint(geom2);
	}

	public static boolean Touches(byte arg0[], byte arg1[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		Geometry geom2 = getGeometry(arg1);
		return geom.touches(geom2);
	}

	public static boolean Within(byte arg0[], byte arg1[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		Geometry geom2 = getGeometry(arg1);
		return geom.within(geom2);
	}

	public static boolean Overlaps(byte arg0[], byte arg1[])
			throws IOException, ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		Geometry geom2 = getGeometry(arg1);
		return geom.overlaps(geom2);
	}

	public static boolean Crosses(byte arg0[], byte arg1[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		Geometry geom2 = getGeometry(arg1);
		return geom.crosses(geom2);
	}

	public static boolean Intersects(byte arg0[], byte arg1[])
			throws IOException, ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		Geometry geom2 = getGeometry(arg1);
		return geom.intersects(geom2);
	}

	public static boolean Contains(byte arg0[], byte arg1[])
			throws IOException, ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		Geometry geom2 = getGeometry(arg1);
		return geom.contains(geom2);
	}

	public static String Relate(byte arg0[], byte arg1[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		Geometry geom2 = getGeometry(arg1);
		return geom.relate(geom2).toString();
	}

	public static double Distance(byte arg0[], byte arg1[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		Geometry geom2 = getGeometry(arg1);
		return geom.distance(geom2);
	}

	public static byte[] Intersection(byte arg0[], byte arg1[])
			throws IOException, ParseException, SQLException,
			ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return setWKBGeometry(geom.intersection(getGeometry(arg1)), geom
				.getSRID());
	}

	public static byte[] GeomDifference(byte arg0[], byte arg1[])
			throws IOException, ParseException, SQLException,
			ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return setWKBGeometry(geom.difference(getGeometry(arg1)), geom
				.getSRID());
	}

	public static byte[] GeomUnion(byte arg0[], byte arg1[])
			throws IOException, ParseException, SQLException,
			ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return setWKBGeometry(geom.union(getGeometry(arg1)), geom.getSRID());
	}

	public static byte[] SymDifference(byte arg0[], byte arg1[])
			throws IOException, ParseException, SQLException,
			ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return setWKBGeometry(geom.symDifference(getGeometry(arg1)), geom
				.getSRID());
	}

	public static byte[] Buffer(byte arg0[], double arg1) throws IOException,
			ParseException, SQLException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		Geometry geomBuffer = geom.buffer(arg1);
		return setWKBGeometry(geomBuffer, geom.getSRID());
	}

	public static byte[] ConvexHull(byte arg0[]) throws IOException,
			ParseException, SQLException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		Geometry result = geom.convexHull();
		return setWKBGeometry(result, geom.getSRID());
	}

	public static boolean IsWithinDistance(byte arg0[], byte arg1[], double arg2)
			throws IOException, ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		Geometry geom2 = getGeometry(arg1);
		return geom.isWithinDistance(geom2, arg2);
	}

	public static boolean IsValid(byte arg0[]) throws IOException,
			ParseException, ClassNotFoundException {
		Geometry geom = getGeometry(arg0);
		return geom.isValid();
	}

	public static String getSpatialTables(Session session) throws SQLException {
		Connection con = session.createConnection(false);
		DatabaseMetaData databaseMeta = con.getMetaData();
		String type[] = { "TABLE", "VIEW" };
		ResultSet tables = databaseMeta.getTables(con.getCatalog(), null, "%",
				type);
		List spatialTableName = new ArrayList();
		while (tables.next()) {
			String tableName = tables.getString("TABLE_NAME");
			String query = (new StringBuilder("select * from ")).append(
					tableName).toString();
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			ResultSetMetaData rsmd = rs.getMetaData();
			int nbCols = rsmd.getColumnCount();
			for (int i = 1; i <= nbCols; i++) {
				String typeSQL = rsmd.getColumnTypeName(i);
				if (typeSQL.equals("GEOMETRY"))
					spatialTableName.add(tableName);
			}

		}
		return spatialTableName.toString();
	}

	static WKBReader wkbreader = new WKBReader();
}