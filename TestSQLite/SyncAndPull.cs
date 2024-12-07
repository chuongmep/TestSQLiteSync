using System.Data.SQLite;
using System.Data.SqlClient;

public class SyncAndPull
{
    private string _sqliteConnectionString = "Data Source=VersionControlDB.sqlite;Version=3;";
    private string _sqlServerConnectionString = "Server=<server name>;Database=<database name>;User Id=your_user;Password=your_password;";

    public void SyncData()
    {
        using (var sqliteConnection = new SQLiteConnection(_sqliteConnectionString))
        {
            sqliteConnection.Open();

            // Pull updates from server first to ensure you're working with the latest data
            PullUpdatesFromServer(sqliteConnection);

            // After ensuring the data is up-to-date, push changes from SQLite to server
            PushChangesToServer(sqliteConnection);
        }
    }

    // Method to pull the latest updates from the server
    private void PullUpdatesFromServer(SQLiteConnection sqliteConnection)
    {
        using (var sqlConnection = new SqlConnection(_sqlServerConnectionString))
        {
            sqlConnection.Open();

            // Get the latest data from the server
            string query = @"
                SELECT id, name, address 
                FROM HistoryData 
                WHERE version = (SELECT MAX(version) FROM HistoryData WHERE id = HistoryData.id)";
            using (var command = new SqlCommand(query, sqlConnection))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    int id = Convert.ToInt32(reader["id"]);
                    string name = reader["name"].ToString();
                    string address = reader["address"].ToString();

                    // Update or insert data into SQLite (this ensures SQLite data is in sync with server)
                    UpdateOrInsertIntoSQLite(sqliteConnection, id, name, address);
                }
            }
        }
    }

    // Method to update or insert data into SQLite
    private void UpdateOrInsertIntoSQLite(SQLiteConnection sqliteConnection, int id, string name, string address)
    {
        string insertMain = "INSERT OR REPLACE INTO MainData (id, name, address) VALUES (@id, @name, @address)";
        using (var command = new SQLiteCommand(insertMain, sqliteConnection))
        {
            command.Parameters.AddWithValue("@id", id);
            command.Parameters.AddWithValue("@name", name);
            command.Parameters.AddWithValue("@address", address);

            command.ExecuteNonQuery();
        }

        // Insert into HistoryData (ensure versioning logic)
        string insertHistory = @"
            INSERT INTO HistoryData (id, name, address, version, changed_at) 
            VALUES (@id, @name, @address, 
                    (SELECT IFNULL(MAX(version), 0) + 1 FROM HistoryData WHERE id = @id), 
                    datetime('now'))";
        using (var command = new SQLiteCommand(insertHistory, sqliteConnection))
        {
            command.Parameters.AddWithValue("@id", id);
            command.Parameters.AddWithValue("@name", name);
            command.Parameters.AddWithValue("@address", address);

            command.ExecuteNonQuery();
        }
    }

    // Method to push changes from SQLite to server
    private void PushChangesToServer(SQLiteConnection sqliteConnection)
    {
        using (var sqlConnection = new SqlConnection(_sqlServerConnectionString))
        {
            sqlConnection.Open();

            // Get the current data from SQLite that needs to be pushed to the server
            string query = "SELECT id, name, address FROM MainData";
            using (var command = new SQLiteCommand(query, sqliteConnection))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    int id = Convert.ToInt32(reader["id"]);
                    string name = reader["name"].ToString();
                    string address = reader["address"].ToString();

                    // Compare with the server's current data and push if necessary
                    PushToServer(id, name, address, sqlConnection);
                }
            }
        }
    }

    // Method to push changes from SQLite to server
    private void PushToServer(int id, string name, string address, SqlConnection sqlConnection)
    {
        string query = @"
            IF EXISTS (SELECT 1 FROM MainData WHERE id = @id)
            BEGIN
                UPDATE MainData
                SET name = @name, address = @address
                WHERE id = @id
            END
            ELSE
            BEGIN
                INSERT INTO MainData (id, name, address)
                VALUES (@id, @name, @address)
            END";

        using (var command = new SqlCommand(query, sqlConnection))
        {
            command.Parameters.AddWithValue("@id", id);
            command.Parameters.AddWithValue("@name", name);
            command.Parameters.AddWithValue("@address", address);

            command.ExecuteNonQuery();
        }
    }
}
