#include <iostream>
#include <filesystem>
#include <fstream>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <string>
#include <cppconn/driver.h>
#include <cppconn/connection.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/exception.h>

class Migration
{
public:
	std::string version;
	std::string upFile;
	std::string downFile;
};

std::string readFile(const std::string& path);
void sortMigrations(std::vector<Migration>&);
void readMigrations(std::vector<Migration>&);
void executeMigrations(sql::Connection&, sql::Statement&, std::vector<Migration>&);

int main()
{
	try
	{
		sql::Driver* driver = get_driver_instance();
		std::unique_ptr<sql::Connection> conn(driver->connect("tcp://127.0.0.1:3306", "root", "password"));
		conn->setSchema("databasename");

		std::unique_ptr<sql::Statement> stmt(conn->createStatement());
		stmt->execute(
			"CREATE TABLE IF NOT EXISTS schema_migrations ("
			"id INT AUTO_INCREMENT PRIMARY KEY, "
			"version VARCHAR(50) NOT NULL, "
			"applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
			"status ENUM('success','failed') DEFAULT 'success',"
			"direction ENUM('up','down') DEFAULT 'up')"
		);

		std::vector<Migration> migrations;

		readMigrations(migrations);

		sortMigrations(migrations);

		executeMigrations(*conn, *stmt, migrations);
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "Database error: " << e.what() << "\n";
		return 1;
	}
	catch (std::exception& e)
	{
		std::cerr << "Error: " << e.what() << "\n";
		return 1;
	}
	return 0;
}

std::string readFile(const std::string& path)
{
	std::ifstream file(path);
	if (!file)
	{
		throw std::runtime_error("Cannot open file: " + path);
	}
	return
	{
		std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()
	};
}

void sortMigrations(std::vector<Migration>& migrations)
{
    bool swapped;
    for (size_t i = 0; i < migrations.size() - 1; ++i)
	{
        swapped = false;
        for (size_t j = 0; j < migrations.size() - i - 1; ++j)
		{
            if (migrations[j].version > migrations[j + 1].version)
			{
                std::swap(migrations[j], migrations[j + 1]);
                swapped = true;
            }
        }
        if (!swapped)
		{
			break;
		}
    }
}

void readMigrations(std::vector<Migration>& migrations)
{
	for (auto& p : std::filesystem::directory_iterator("migrations"))
	{
		if (p.path().string().find("_up.sql") != std::string::npos)
		{
			std::string filename = p.path().filename().string();
			std::string version = filename.substr(0, 3);
			std::string upFile = p.path().string();
			std::string downFile = p.path().string();
			size_t pos = downFile.find("_up.sql");
			if (pos != std::string::npos) {
				downFile.replace(pos, 7, "_down.sql");
			}

			migrations.push_back({ version, upFile, downFile });
		}
	}
}

void executeMigrations(sql::Connection& conn, sql::Statement& stmt, std::vector<Migration>& migrations)
{
	for (auto& mig : migrations)
	{

		std::unique_ptr<sql::PreparedStatement> checkStmt(
			conn.prepareStatement("SELECT 1 FROM schema_migrations WHERE version = ? AND status='success' AND direction='up'")
		);

		checkStmt->setString(1, mig.version);

		std::unique_ptr<sql::ResultSet> res(checkStmt->executeQuery());
		if (res->next()) {
			std::cout << "Skipping migration " << mig.version << " (already applied)\n";
			continue;
		}

		std::string sql = readFile(mig.upFile);
		try {

			stmt.execute("START TRANSACTION");
			stmt.execute(sql);
			std::unique_ptr<sql::PreparedStatement> insertStmt(
				conn.prepareStatement("INSERT INTO schema_migrations (version, status, direction) VALUES (?, 'success', 'up')")
			);
			insertStmt->setString(1, mig.version);
			insertStmt->execute();
			stmt.execute("COMMIT");
			std::cout << "Applied migration " << mig.version << "\n";
		}
		catch (sql::SQLException& e) {
			stmt.execute("ROLLBACK");
			std::cerr << "Migration " << mig.version << " failed: " << e.what() << "\n";
			std::ofstream dlq("migration_dlq.log", std::ios::app);
			dlq << mig.version << ": " << e.what() << "\n";

			std::unique_ptr<sql::PreparedStatement> failStmt(
				conn.prepareStatement("INSERT INTO schema_migrations (version, status, direction) VALUES (?, 'failed', 'up') "
					"ON DUPLICATE KEY UPDATE status='failed'")
			);
			failStmt->setString(1, mig.version);
			failStmt->execute();

			break;
		}
	}
}