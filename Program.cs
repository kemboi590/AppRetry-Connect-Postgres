using System;
using System.Data;
using System.IO;
using System.Text;
using System.Threading;
using Npgsql;

namespace Driver
{
    public class CosmosPostgresCRUD
    {
        // Connection string setup
        static string connStr = new NpgsqlConnectionStringBuilder("Server = c-csharp-postgres-sdk.f6ffz4zwmfixzj.postgres.cosmos.azure.com; Database = citus; Port= 5432; User Id = citus; Password = kemboi590@; Ssl Mode = Require; Pooling = true; Minimum Pool Size = 0; Maximum Pool Size = 50;").ToString();

        // Retry logic for executing SQL commands
        static void executeWithRetry(Action<NpgsqlConnection> dbAction, int retryCount = 5)
        {
            for (int i = 0; i < retryCount; i++)
            {
                try
                {
                    using (var conn = new NpgsqlConnection(connStr))
                    {
                        conn.Open();
                        dbAction(conn); // Execute the provided database action
                        return; // Exit if successful
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Attempt {i + 1} failed: {e.Message}");
                    Thread.Sleep(60000); // Wait before retrying
                }
            }
            Console.WriteLine("Operation failed after multiple retries.");
        }

        static void Main(string[] args)
        {
            // Dropping the table and creating it again with retry
            executeWithRetry(conn =>
            {
                Console.Out.WriteLine("Opening connection");

                using (var command = new NpgsqlCommand("DROP TABLE IF EXISTS pharmacy;", conn))
                {
                    command.ExecuteNonQuery();
                    Console.Out.WriteLine("Finished dropping table (if existed)");
                }
                using (var command = new NpgsqlCommand("CREATE TABLE pharmacy (pharmacy_id integer ,pharmacy_name text,city text,state text,zip_code integer);", conn))
                {
                    command.ExecuteNonQuery();
                    Console.Out.WriteLine("Finished creating table");
                }
                using (var command = new NpgsqlCommand("CREATE INDEX idx_pharmacy_id ON pharmacy(pharmacy_id);", conn))
                {
                    command.ExecuteNonQuery();
                    Console.Out.WriteLine("Finished creating index");
                }

                // Insert data into the table
                using (var command = new NpgsqlCommand("INSERT INTO pharmacy (pharmacy_id, pharmacy_name, city, state, zip_code) VALUES (@n1, @q1, @a, @b, @c);", conn))
                {
                    command.Parameters.AddWithValue("n1", 0);
                    command.Parameters.AddWithValue("q1", "Target");
                    command.Parameters.AddWithValue("a", "Sunnyvale");
                    command.Parameters.AddWithValue("b", "California");
                    command.Parameters.AddWithValue("c", 94001);

                    int nRows = command.ExecuteNonQuery();
                    Console.Out.WriteLine($"Number of rows inserted={nRows}");
                }
            });

            // Distribute the table with retry
            executeWithRetry(conn =>
            {
                using (var command = new NpgsqlCommand("select create_distributed_table('pharmacy', 'pharmacy_id');", conn))
                {
                    command.ExecuteNonQuery();
                    Console.Out.WriteLine("Finished distributing the table");
                }
            });

            // Read and update data with retry
            executeWithRetry(conn =>
            {
                // Read data
                using (var command = new NpgsqlCommand("SELECT * FROM pharmacy;", conn))
                {
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        Console.WriteLine($"Reading from table = ({reader.GetInt32(0)}, {reader.GetString(1)}, {reader.GetString(2)}, {reader.GetString(3)}, {reader.GetInt32(4)})");
                    }
                    reader.Close();
                }

                // Update data
                using (var command = new NpgsqlCommand("UPDATE pharmacy SET city = @q WHERE pharmacy_id = @n;", conn))
                {
                    command.Parameters.AddWithValue("n", 0);
                    command.Parameters.AddWithValue("q", "Nairobi");
                    int nRows = command.ExecuteNonQuery();
                    Console.Out.WriteLine($"Number of rows updated={nRows}");
                }
            });

            // Delete data with retry
            executeWithRetry(conn =>
            {
                using (var command = new NpgsqlCommand("DELETE FROM pharmacy WHERE pharmacy_id = @n;", conn))
                {
                    command.Parameters.AddWithValue("n", 0);
                    int nRows = command.ExecuteNonQuery();
                    Console.Out.WriteLine($"Number of rows deleted={nRows}");
                }
            });

            // Copy data from a file to the database with retry
            executeWithRetry(conn =>
            {
                String sDestinationSchemaAndTableName = "pharmacy";
                String sFromFilePath = "C:\\Users\\KEMBOI\\Documents\\pharmacies.csv";

                if (File.Exists(sFromFilePath))
                {
                    using (var writer = conn.BeginTextImport($"COPY {sDestinationSchemaAndTableName} FROM STDIN WITH (FORMAT CSV, HEADER true, NULL '');"))
                    {
                        foreach (String sLine in File.ReadLines(sFromFilePath))
                        {
                            writer.WriteLine(sLine);
                        }
                    }
                    Console.WriteLine("Data loaded successfully");
                }
            });
        }
    }
}
