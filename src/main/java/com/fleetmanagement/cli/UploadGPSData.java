package com.fleetmanagement.cli;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

/**
 * The `UploadGPSData` class is responsible for uploading GPS data from CSV files to a PostgreSQL database.
 * It supports two types of data: taxis and trajectories.
 *
 * <p>Usage:</p>
 * <pre>
 * MAVEN
 * mvn clean compile
 * mvn exec:java -Dexec.mainClass="com.fleetmanagement.cli.UploadGPSData" -Dexec.args="src/main/resources/data/taxis/ --type=taxis --dbname=api_fleet_db --host=localhost --port=5432 --username=api_admin"
 * mvn exec:java -Dexec.mainClass="com.fleetmanagement.cli.UploadGPSData" -Dexec.args="src/main/resources/data/trajectories-02/ --type=trajectories --dbname=api_fleet_db --host=localhost --port=5432 --username=api_admin"
 * </pre>
 */
public class UploadGPSData {
	private static final int BATCH_SIZE = 1000;

	public static void main(String[] args) throws ClassNotFoundException {
		String dirPath = null;
		String type = null;
		String dbName = null;
		String host = null;
		int port = 0;
		String username = null;
		String password;

		for (String arg : args) {
			if (arg.startsWith("--type=")) {
				type = arg.split("=")[1];
			} else if (arg.startsWith("--dbname=")) {
				dbName = arg.split("=")[1];
			} else if (arg.startsWith("--host=")) {
				host = arg.split("=")[1];
			} else if (arg.startsWith("--port=")) {
				port = Integer.parseInt(arg.split("=")[1]);
			} else if (arg.startsWith("--username=")) {
				username = arg.split("=")[1];
			} else {
				dirPath = arg;
			}
		}

		Console console = System.console();
		char[] passwordArray = console.readPassword("Enter password: ");
		password = new String(passwordArray);

		System.out.println("File path: " + dirPath);
		System.out.println("Type: " + type);
		System.out.println("Database name: " + dbName);
		System.out.println("Host: " + host);
		System.out.println("Port: " + port);
		System.out.println("Password: " + (password != null ? "*******" : "Not provided"));

		String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, dbName);

		Class.forName("org.postgresql.Driver");

		// Connect to the database
		try {
			Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
			System.out.println("Connected to the PostgreSQL server successfully!");

			// Validate taxi IDs
			Set<Integer> validTaxiIds = loadAllTaxiIds(connection);

			int fileCounter = 0;

			File directory = new File(dirPath);
			if (directory.isDirectory()) {
				File[] files = directory.listFiles();
				int filesLength = files.length;
				if (files != null) {
					for (File file : files) {
						if (file.isFile()) {
							System.out.println(
									"Processing file: " + file.getName() + " (" + ++fileCounter + "/" + filesLength +
											")");
							if (type.equals("taxis")) {
								copyTaxiData(connection, file.getPath());
							} else if (type.equals("trajectories")) {
								insertTrajectoriesData(connection, file.getPath(), validTaxiIds);
							}
						}
					}
				}
			} else {
				System.out.println("Invalid directory path.");
			}
			connection.close();
		} catch (SQLException e) {
			System.out.println("Failed to connect to the PostgreSQL server.");
			System.out.println(e.getMessage());
		}
	}

	/**
	 * Loads all taxi IDs from the database.
	 *
	 * @param connection the database connection
	 * @return a set of all taxi IDs
	 */
	private static Set<Integer> loadAllTaxiIds(Connection connection) {
		Set<Integer> taxiIds = new HashSet<>();
		String selectSQL = "SELECT id FROM api.taxis";
		try (Statement statement = connection.createStatement();
			 ResultSet resultSet = statement.executeQuery(selectSQL)) {
			while (resultSet.next()) {
				taxiIds.add(resultSet.getInt("id"));
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error executing SELECT for taxi ids. ", e);
		}
		System.out.println("Loaded total of taxi IDs: " + taxiIds.size());
		return taxiIds;
	}

	/**
	 * Copies taxi data from a CSV file to the PostgreSQL database.
	 *
	 * @param connection the database connection
	 * @param filePath   the path to the CSV file containing taxi data
	 */
	private static void copyTaxiData(Connection connection, String filePath) {
		String copySQL = "COPY api.taxis (id, plate) FROM STDIN WITH (FORMAT csv)";

		try {
			FileReader fileReader = new FileReader(filePath);
			CopyManager copyManager = new CopyManager((BaseConnection) connection);
			copyManager.copyIn(copySQL, fileReader);

			System.out.println("Taxi data copied successfully for file: " + filePath);
		} catch (SQLException | IOException e) {
			throw new RuntimeException("Error executing COPY for file: " + filePath, e);
		}
	}

	/**
	 * Inserts trajectory data from a CSV file into the PostgreSQL database.
	 *
	 * @param connection   the database connection
	 * @param filePath     the path to the CSV file containing trajectory data
	 * @param validTaxiIds a set of valid taxi IDs to filter the data
	 */
	private static void insertTrajectoriesData(Connection connection, String filePath, Set<Integer> validTaxiIds) {
		String insertSQL = "INSERT INTO api.trajectories (taxi_id, date, latitude, longitude) VALUES (?, ?, ?, ?)";

		int batchCount = 0;

		try (BufferedReader reader = new BufferedReader(new FileReader(filePath));
			 PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
			String line;
			int rowCount = 0;

			while ((line = reader.readLine()) != null) {
				String[] data = line.split(",");
				int taxiId = Integer.parseInt(data[0]);

				if (validTaxiIds.contains(taxiId)) {
					preparedStatement.setInt(1, taxiId);
					preparedStatement.setTimestamp(2, Timestamp.valueOf(data[1]));
					preparedStatement.setDouble(3, Double.parseDouble(data[2]));
					preparedStatement.setDouble(4, Double.parseDouble(data[3]));
					preparedStatement.addBatch();

					rowCount++;

					if (rowCount % BATCH_SIZE == 0) {
						preparedStatement.executeBatch();
						batchCount++;
						System.out.println("Executed batch " + batchCount + " for file: " + filePath);
					}
				}
			}
			preparedStatement.executeBatch();
			System.out.println("Trajectories data inserted successfully for file: " + filePath);
		} catch (SQLException | IOException e) {
			throw new RuntimeException("Error executing COPY for file: " + filePath, e);
		}
	}
}