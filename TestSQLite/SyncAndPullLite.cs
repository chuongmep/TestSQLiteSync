using System;
using System.Data.SQLite;
using System.IO;

class SyncAndPullLite
{
    // Connection strings for both databases
    private string _sqliteConnectionString1 = "Data Source=VersionControlDB.sqlite;Version=3;";  // Local SQLite
    private string _sqliteConnectionString2 = "Data Source=VersionControlDBServer.sqlite;Version=3;";  // Server SQLite

    public void SyncData()
    {
        using (var sqliteConnection1 = new SQLiteConnection(_sqliteConnectionString1))
        using (var sqliteConnection2 = new SQLiteConnection(_sqliteConnectionString2))
        {
            try
            {
                // Check if databases exist
                CheckDatabaseExistence(_sqliteConnectionString1);
                CheckDatabaseExistence(_sqliteConnectionString2);

                // Ensure the databases and tables are created
                CreateDatabaseAndTablesIfNeeded(sqliteConnection1);
                CreateDatabaseAndTablesIfNeeded(sqliteConnection2);

                // Open the connections
                OpenConnectionIfNeeded(sqliteConnection1);
                OpenConnectionIfNeeded(sqliteConnection2);

                // After ensuring SQLite1 is up-to-date, push changes from SQLite1 to SQLite2
                PushChangesToSQLite(sqliteConnection1, sqliteConnection2);

                // Pull updates from SQLite2 (server) to SQLite1 (local)
                PullUpdatesFromSQLite(sqliteConnection2, sqliteConnection1);
                // Keep only the latest 5 versions for each ID in the HistoryData table
                KeepLatestFiveVersions(sqliteConnection2);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }
    }

    // Check if the SQLite database file exists
    private void CheckDatabaseExistence(string connectionString)
    {
        string databasePath = new SQLiteConnectionStringBuilder(connectionString).DataSource;
        if (!File.Exists(databasePath))
        {
            Console.WriteLine($"Database file does not exist at: {databasePath}");
        }
    }

    // Ensure the database exists and the required tables are created
    private void CreateDatabaseAndTablesIfNeeded(SQLiteConnection connection)
    {
        try
        {
            if (connection.State != System.Data.ConnectionState.Open)
            {
                connection.Open();
            }

            // Create MainData table if not exists
            string createMainDataTableQuery = @"
                CREATE TABLE IF NOT EXISTS MainData (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    address TEXT
                )";
            using (var command = new SQLiteCommand(createMainDataTableQuery, connection))
            {
                command.ExecuteNonQuery();
            }

            // Create HistoryData table if not exists
            string createHistoryDataTableQuery = @"
                CREATE TABLE IF NOT EXISTS HistoryData (
                    id INTEGER,
                    name TEXT,
                    address TEXT,
                    version INTEGER,
                    changed_at TEXT,
                    PRIMARY KEY (id, version)
                )";
            using (var command = new SQLiteCommand(createHistoryDataTableQuery, connection))
            {
                command.ExecuteNonQuery();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error creating tables: {ex.Message}");
        }
    }

    // Method to open the SQLite connection if it's not already open
    private void OpenConnectionIfNeeded(SQLiteConnection connection)
    {
        try
        {
            if (connection.State != System.Data.ConnectionState.Open)
            {
                connection.Open();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error opening connection: {ex.Message}");
        }
    }

    // Pull updates from server database (SQLite2) to local database (SQLite1)
    private void PullUpdatesFromSQLite(SQLiteConnection sourceConnection, SQLiteConnection destinationConnection)
    {
        string selectQuery = "SELECT * FROM MainData";
        using (var command = new SQLiteCommand(selectQuery, sourceConnection))
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                // Extract data from the source
                int id = Convert.ToInt32(reader["id"]);
                string name = reader["name"].ToString();
                string address = reader["address"].ToString();

                // Insert or update the data in the destination database
                string upsertQuery = @"
                    INSERT INTO MainData (id, name, address)
                    VALUES (@id, @name, @address)
                    ON CONFLICT(id) 
                    DO UPDATE SET 
                        name = @name, 
                        address = @address";
                using (var insertCommand = new SQLiteCommand(upsertQuery, destinationConnection))
                {
                    insertCommand.Parameters.AddWithValue("@id", id);
                    insertCommand.Parameters.AddWithValue("@name", name);
                    insertCommand.Parameters.AddWithValue("@address", address);
                    insertCommand.ExecuteNonQuery();
                }

                // Insert/update into HistoryData table to keep track of changes
                string historyInsertQuery = @"
                    INSERT INTO HistoryData (id, name, address, version, changed_at)
                    VALUES (@id, @name, @address, (SELECT IFNULL(MAX(version), 0) + 1 FROM HistoryData WHERE id = @id), @changedAt)";
                using (var historyCommand = new SQLiteCommand(historyInsertQuery, destinationConnection))
                {
                    historyCommand.Parameters.AddWithValue("@id", id);
                    historyCommand.Parameters.AddWithValue("@name", name);
                    historyCommand.Parameters.AddWithValue("@address", address);
                    historyCommand.Parameters.AddWithValue("@changedAt", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                    historyCommand.ExecuteNonQuery();
                }
            }
        }
    }

    // Push changes from local database (SQLite1) to server database (SQLite2)
    private void PushChangesToSQLite(SQLiteConnection sourceConnection, SQLiteConnection destinationConnection)
    {
        string selectQuery = "SELECT * FROM MainData";
        using (var command = new SQLiteCommand(selectQuery, sourceConnection))
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                // Extract data from the source (SQLite1)
                int id = Convert.ToInt32(reader["id"]);
                string name = reader["name"].ToString();
                string address = reader["address"].ToString();

                // Insert or update the data in the destination (SQLite2)
                string upsertQuery = @"
                    INSERT INTO MainData (id, name, address)
                    VALUES (@id, @name, @address)
                    ON CONFLICT(id) 
                    DO UPDATE SET 
                        name = @name, 
                        address = @address";
                using (var insertCommand = new SQLiteCommand(upsertQuery, destinationConnection))
                {
                    insertCommand.Parameters.AddWithValue("@id", id);
                    insertCommand.Parameters.AddWithValue("@name", name);
                    insertCommand.Parameters.AddWithValue("@address", address);
                    insertCommand.ExecuteNonQuery();
                }

                // Insert/update into HistoryData table to keep track of changes
                string historyInsertQuery = @"
                    INSERT INTO HistoryData (id, name, address, version, changed_at)
                    VALUES (@id, @name, @address, (SELECT IFNULL(MAX(version), 0) + 1 FROM HistoryData WHERE id = @id), @changedAt)";
                using (var historyCommand = new SQLiteCommand(historyInsertQuery, destinationConnection))
                {
                    historyCommand.Parameters.AddWithValue("@id", id);
                    historyCommand.Parameters.AddWithValue("@name", name);
                    historyCommand.Parameters.AddWithValue("@address", address);
                    historyCommand.Parameters.AddWithValue("@changedAt", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                    historyCommand.ExecuteNonQuery();
                }
            }
        }
    }
     static void KeepLatestFiveVersions(SQLiteConnection connection)
    {
        // Start a transaction to ensure atomicity
        using (var transaction = connection.BeginTransaction())
        {
            try
            {
                // Get all distinct IDs from the HistoryData table
                string getDistinctIdsQuery = "SELECT DISTINCT id FROM HistoryData";
                using (var command = new SQLiteCommand(getDistinctIdsQuery, connection, transaction))
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int id = Convert.ToInt32(reader["id"]);
                        Console.WriteLine($"Processing ID: {id}");

                        // Delete versions older than the latest 5 versions for each ID
                        DeleteOlderVersions(connection, transaction, id);
                    }
                }

                // Commit the transaction
                transaction.Commit();
            }
            catch (Exception ex)
            {
                // Rollback in case of error
                transaction.Rollback();
                Console.WriteLine($"Error keeping the latest 5 versions: {ex.Message}");
            }
        }
    }

    static void DeleteOlderVersions(SQLiteConnection connection, SQLiteTransaction transaction, int id)
    {
        // Get the latest 2 versions for the current ID
        string getLatestVersionsQuery = @"
        SELECT version FROM HistoryData
        WHERE id = @id
        ORDER BY version DESC
        LIMIT 2";
        using (var command = new SQLiteCommand(getLatestVersionsQuery, connection, transaction))
        {
            command.Parameters.AddWithValue("@id", id);
            using (var reader = command.ExecuteReader())
            {
                // Store the latest 5 versions in a list
                var latestVersions = new System.Collections.Generic.List<int>();
                while (reader.Read())
                {
                    latestVersions.Add(Convert.ToInt32(reader["version"]));
                }

                if (latestVersions.Count > 0)
                {
                    // Delete all versions older than the latest 5 versions
                    string deleteOlderVersionsQuery = @"
                    DELETE FROM HistoryData
                    WHERE id = @id AND version NOT IN (" + string.Join(",", latestVersions) + ")";
                    using (var deleteCommand = new SQLiteCommand(deleteOlderVersionsQuery, connection, transaction))
                    {
                        deleteCommand.Parameters.AddWithValue("@id", id);
                        int rowsDeleted = deleteCommand.ExecuteNonQuery();
                        Console.WriteLine($"Deleted {rowsDeleted} older versions for ID: {id}");
                    }
                }
            }
        }
    }
}
