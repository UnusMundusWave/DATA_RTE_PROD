# French Nuclear Power Production Monitoring Project

This project consists of a set of Python scripts and a batch file designed to monitor and report on energy production data. It retrieves data from an API, processes it, stores it in a SQLite database, and sends reports via Telegram.

## Project Structure

-   `_0_production_monitoring.bat`: Batch file that orchestrates the execution of the Python scripts.
-   `_1_getTransparencyAPI.py`: Python script that retrieves generation data from the ENTSO-E Transparency Platform API, saves it to a CSV file, and handles potential connection errors with a retry mechanism.
-   `_2_parser_csv.py`: Python script that parses the raw CSV data, filters it to include only nuclear production units, performs data cleaning and transformations, and saves the processed data to a new CSV file.
-   `_3_import_csv.py`: Python script that imports the filtered CSV data into a SQLite database (`production.db`), creating the database and tables if they don't exist. It also handles data type conversions and ensures data integrity.
-   `_4_ProductionReporting_Telegram_bot.py`: Python script that queries the SQLite database to generate production reports, identifies units with low production, and sends the reports via Telegram.
-   `.env`: Environment file to store API keys and other configuration variables.
-   `production.db`: SQLite database to store the production data.
-   `Readme.md`: Documentation file for the project.

## Prerequisites

Before running the project, ensure you have the following installed:

-   Python 3.x
-   pip (Python package installer)

## Installation

1.  Clone the repository:

    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  Create a virtual environment:

    ```bash
    python -m venv venv
    ```

3.  Activate the virtual environment:

    -   On Windows:

        ```bash
        .\venv\Scripts\activate
        ```

    -   On macOS and Linux:

        ```bash
        source venv/bin/activate
        ```

4.  Install the required Python packages:

    ```bash
    pip install pandas entsoe python-dotenv pytz tenacity python-telegram-bot dateparser apprise
    ```

## Configuration

1.  Create a [.env](http://_vscodecontentref_/8) file in the project root directory.
2.  Add the following variables to the [.env](http://_vscodecontentref_/9) file, replacing the placeholders with your actual values:

    ```
    API_TOKEN=<YOUR_ENTSOE_API_TOKEN>
    TELEGRAM_BOT_TOKEN=<YOUR_TELEGRAM_BOT_TOKEN>
    TELEGRAM_CHAT_ID=<YOUR_TELEGRAM_CHAT_ID>
    DATA_DIRECTORY=<PATH_TO_DATA_DIRECTORY>
    ```

    -   `API_TOKEN`: Your API token for the Entsoe Transparency Platform.
    -   [TELEGRAM_BOT_TOKEN](http://_vscodecontentref_/10): Your Telegram bot token.
    -   `TELEGRAM_CHAT_ID`: The chat ID where the bot will send messages.
    -   [DATA_DIRECTORY](http://_vscodecontentref_/11): The directory where CSV files will be stored.

## Usage

1.  Run the [_0_production_monitoring.bat](http://_vscodecontentref_/12) batch file to start the data retrieval, parsing, and reporting cycle.

    ```bash
    _0_production_monitoring.bat
    ```

    The batch file executes the Python scripts in the following order:

    1.  [_1_getTransparencyAPI.py](http://_vscodecontentref_/13): Retrieves data from the Entsoe API and saves it to a CSV file in the directory specified by the [DATA_DIRECTORY](http://_vscodecontentref_/14) environment variable.
    2.  [_2_parser_csv.py](http://_vscodecontentref_/15): Parses the CSV file, filters the data, and saves the filtered data to a new CSV file.
    3.  [_3_import_csv.py](http://_vscodecontentref_/16): Imports the filtered CSV data into the SQLite database (`production.db`).
    4.  [_4_ProductionReporting_Telegram_bot.py](http://_vscodecontentref_/17): Generates a production report and sends it to the specified Telegram chat ID.

The script is configured to run every XX:50 as defined by the `TARGET_MINUTE` variable in the [_0_production_monitoring.bat](http://_vscodecontentref_/18) file.

## Database Schema

The project uses a SQLite database (`production.db`) to store the production data. The database contains the following tables:

-   **units:** Stores information about the production units.

    ```sql
    CREATE TABLE units (
        id INTEGER PRIMARY KEY AUTOINCREMENT, -- Identifiant unique de l'unité
        name TEXT NOT NULL,                   -- Nom de l'unité
        location TEXT NOT NULL,               -- Lieu de l'unité
        production_type TEXT NOT NULL,        -- Type de production (e.g., énergie, alimentaire)
        installation_date DATE,               -- Date d'installation
        characteristics TEXT,                  -- Autres caractéristiques (JSON ou texte formaté)
        nominal REAL                          -- Production nominale
    );
    ```

-   **production:** Stores the production data for each unit at specific timestamps.

    ```sql
    CREATE TABLE IF NOT EXISTS "production"(
        id INT,
        unit_id INT,
        timestamp NUM,
        value REAL
    );
    ```

-   **centrales:** Stores information about the power plants.

    ```sql
    CREATE TABLE IF NOT EXISTS "centrales" (
        id INTEGER PRIMARY KEY AUTOINCREMENT, -- Identifiant unique de la centrale
        name TEXT NOT NULL,                   -- Nom de la centrale
        unit_ids TEXT NOT NULL,               -- Liste des IDs des unités installées dans cette centrale
        position TEXT                         -- Coordonnées de la centrale (au format JSON)
    );
    ```

## Indices

The following indices are defined to optimize query performance:

```sql
CREATE INDEX idx_production_unit_time
ON production(unit_id, timestamp DESC);

CREATE INDEX idx_units_nominal
ON units(id, nominal);