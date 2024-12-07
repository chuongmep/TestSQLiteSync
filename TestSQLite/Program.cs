using System.Data.SQLite;

class VersionControlExample
{
    static void Main(string[] args)
    {
        string connectionString = "Data Source=VersionControlDB.sqlite;Version=3;";

        // Ensure database and tables exist
        InitializeDatabase(connectionString);

        using (var connection = new SQLiteConnection(connectionString))
        {
            connection.Open();

            // Add new data
            AddNewData(connection, 1, "John Doe", "123 Street A");
            AddNewData(connection, 2, "Jane Smith", "456 Street B");
            AddNewData(connection, 3, "me", "123 Stress C");

            // Update data
            UpdateData(connection, 3, "me", "456 Updated Street C");

            // Rollback data
            //RollbackData(connection, 1, 1);

            // just keep the latest 5 versions
            KeepLatestFiveVersions(connection);

            // Display current and historical data
            DisplayData(connection);
            connection.Close();

            // SyncAndPullLite
            Console.WriteLine("\nSyncing data with server...");
            SyncAndPullLite syncAndPullLite = new SyncAndPullLite();
            syncAndPullLite.SyncData();
        }
    }

    static void InitializeDatabase(string connectionString)
    {
        using (var connection = new SQLiteConnection(connectionString))
        {
            connection.Open();

            string createMainTable = @"
                CREATE TABLE IF NOT EXISTS MainData (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    address TEXT NOT NULL
                )";

            string createHistoryTable = @"
                CREATE TABLE IF NOT EXISTS HistoryData (
                    id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    address TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    changed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id, version)
                )";

            using (var command = new SQLiteCommand(createMainTable, connection))
                command.ExecuteNonQuery();

            using (var command = new SQLiteCommand(createHistoryTable, connection))
                command.ExecuteNonQuery();
        }
    }

    static void AddNewData(SQLiteConnection connection, int id, string name, string address)
{
    using (var transaction = connection.BeginTransaction())
    {
        try
        {
            // Check if the ID already exists in MainData
            string checkId = "SELECT COUNT(*) FROM MainData WHERE id = @id";
            using (var checkCommand = new SQLiteCommand(checkId, connection, transaction))
            {
                checkCommand.Parameters.AddWithValue("@id", id);
                int count = Convert.ToInt32(checkCommand.ExecuteScalar());

                if (count > 0)
                {
                    Console.WriteLine($"ID {id} already exists in MainData. Skipping insertion.");
                    transaction.Rollback();
                    return;
                }
            }

            // Insert into MainData
            string insertMain = "INSERT INTO MainData (id, name, address) VALUES (@id, @name, @address)";
            using (var command = new SQLiteCommand(insertMain, connection, transaction))
            {
                command.Parameters.AddWithValue("@id", id);
                command.Parameters.AddWithValue("@name", name);
                command.Parameters.AddWithValue("@address", address);
                command.ExecuteNonQuery();
            }

            // Insert initial version into HistoryData
            string insertHistory = "INSERT INTO HistoryData (id, name, address, version) VALUES (@id, @name, @address, 1)";
            using (var command = new SQLiteCommand(insertHistory, connection, transaction))
            {
                command.Parameters.AddWithValue("@id", id);
                command.Parameters.AddWithValue("@name", name);
                command.Parameters.AddWithValue("@address", address);
                command.ExecuteNonQuery();
            }

            transaction.Commit();
            Console.WriteLine($"Added new data for ID: {id}");
        }
        catch (Exception ex)
        {
            transaction.Rollback();
            Console.WriteLine($"Error adding new data for ID: {id} - {ex.Message}");
        }
    }
}


    static void UpdateData(SQLiteConnection connection, int id, string newName, string newAddress)
    {
        using (var transaction = connection.BeginTransaction())
        {
            try
            {
                // Get current data
                string selectMain = "SELECT * FROM MainData WHERE id = @id";
                using (var command = new SQLiteCommand(selectMain, connection, transaction))
                {
                    command.Parameters.AddWithValue("@id", id);
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            string currentName = reader["name"].ToString();
                            string currentAddress = reader["address"].ToString();

                            // Get current max version from HistoryData
                            string getMaxVersion = "SELECT IFNULL(MAX(version), 0) FROM HistoryData WHERE id = @id";
                            using (var versionCommand = new SQLiteCommand(getMaxVersion, connection, transaction))
                            {
                                versionCommand.Parameters.AddWithValue("@id", id);
                                int maxVersion = Convert.ToInt32(versionCommand.ExecuteScalar());

                                // Insert current data into HistoryData
                                string insertHistory = "INSERT INTO HistoryData (id, name, address, version) VALUES (@id, @name, @address, @version)";
                                using (var historyCommand = new SQLiteCommand(insertHistory, connection, transaction))
                                {
                                    historyCommand.Parameters.AddWithValue("@id", id);
                                    historyCommand.Parameters.AddWithValue("@name", currentName);
                                    historyCommand.Parameters.AddWithValue("@address", currentAddress);
                                    historyCommand.Parameters.AddWithValue("@version", maxVersion + 1);
                                    historyCommand.ExecuteNonQuery();
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine($"No data found for ID: {id}");
                            transaction.Rollback();
                            return;
                        }
                    }
                }

                // Update MainData with new data
                string updateMain = "UPDATE MainData SET name = @name, address = @address WHERE id = @id";
                using (var updateCommand = new SQLiteCommand(updateMain, connection, transaction))
                {
                    updateCommand.Parameters.AddWithValue("@id", id);
                    updateCommand.Parameters.AddWithValue("@name", newName);
                    updateCommand.Parameters.AddWithValue("@address", newAddress);
                    updateCommand.ExecuteNonQuery();
                }

                transaction.Commit();
                Console.WriteLine($"Updated data for ID: {id}");
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                Console.WriteLine($"Error updating data for ID: {id} - {ex.Message}");
            }
        }
    }

    static void RollbackData(SQLiteConnection connection, int id, int version)
    {
        string selectHistory = "SELECT * FROM HistoryData WHERE id = @id AND version = @version";
        using (var command = new SQLiteCommand(selectHistory, connection))
        {
            command.Parameters.AddWithValue("@id", id);
            command.Parameters.AddWithValue("@version", version);
            using (var reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    string name = reader["name"].ToString();
                    string address = reader["address"].ToString();

                    // Rollback MainData to historical data
                    string rollbackMain = "UPDATE MainData SET name = @name, address = @address WHERE id = @id";
                    using (var rollbackCommand = new SQLiteCommand(rollbackMain, connection))
                    {
                        rollbackCommand.Parameters.AddWithValue("@id", id);
                        rollbackCommand.Parameters.AddWithValue("@name", name);
                        rollbackCommand.Parameters.AddWithValue("@address", address);
                        rollbackCommand.ExecuteNonQuery();
                    }

                    Console.WriteLine($"Rolled back data for ID: {id} to version: {version}");
                }
                else
                {
                    Console.WriteLine($"No historical data found for ID: {id}, version: {version}");
                }
            }
        }
    }

    static void DisplayData(SQLiteConnection connection)
    {
        Console.WriteLine("\nCurrent Data in MainData:");
        string selectMain = "SELECT * FROM MainData";
        using (var command = new SQLiteCommand(selectMain, connection))
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                Console.WriteLine($"ID: {reader["id"]}, Name: {reader["name"]}, Address: {reader["address"]}");
            }
        }

        Console.WriteLine("\nHistorical Data in HistoryData:");
        string selectHistory = "SELECT * FROM HistoryData";
        using (var command = new SQLiteCommand(selectHistory, connection))
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                Console.WriteLine($"ID: {reader["id"]}, Name: {reader["name"]}, Address: {reader["address"]}, Version: {reader["version"]}, Changed At: {reader["changed_at"]}");
            }
        }
    }
    //static void DeleteHistoryData30DaysOld(SQLiteConnection connection)
    // {
    //     string deleteHistory = "DELETE FROM HistoryData WHERE changed_at < datetime('now', '-30 days')";
    //     using (var command = new SQLiteCommand(deleteHistory, connection))
    //     {
    //         int rowsDeleted = command.ExecuteNonQuery();
    //         Console.WriteLine($"Deleted {rowsDeleted} rows from HistoryData older than 30 days");
    //     }
    // }
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
        // Get the latest 5 versions for the current ID
        string getLatestVersionsQuery = @"
        SELECT version FROM HistoryData
        WHERE id = @id
        ORDER BY version DESC
        LIMIT 5";
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
