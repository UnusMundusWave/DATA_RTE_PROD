# Production Monitoring Project

This project consists of a set of Python scripts and a batch file designed to monitor and report on energy production data. It retrieves data from an API, processes it, stores it in a SQLite database, and sends reports via Telegram.

## Project Structure

-   `_0_production_monitoring.bat`: Batch file that orchestrates the execution of the Python scripts.
-   `_1_getTransparencyAPI.py`: Python script to retrieve data from the Entsoe Transparency API.
-   `_2_parser_csv.py`: Python script to parse and filter the CSV data.
-   `_3_import_csv.py`: Python script to import the parsed CSV data into a SQLite database.
-   `_4_ProductionReporting_Telegram_bot.py`: Python script to generate production reports and send them via Telegram.
-   `.env`: Environment file to store API keys and other configuration variables.
-   `.gitignore`: Specifies intentionally untracked files that Git should ignore.
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

## Database

The project uses a SQLite database (`production.db`) to store the production data. The database contains two tables:

-   [units](http://_vscodecontentref_/19): Stores information about the production units.
-   `production`: Stores the production data for each unit at specific timestamps.

## Logging

The Python scripts use the [logging](http://_vscodecontentref_/20) module to log information, warnings, and errors. Logs are displayed in the console.

## Error Handling

The batch file and Python scripts include error handling to catch and report any issues that occur during execution. If an error occurs, the batch file will pause and display an error message. The Python scripts will log errors and exit.

## Notes

-   Ensure that the required environment variables are set correctly in the [.env](http://_vscodecontentref_/21) file.
-   Check the logs for any errors or warnings during execution.
-   The [DATA_DIRECTORY](http://_vscodecontentref_/22) should exist, or the scripts will create it.
-   The Telegram bot needs to be started and authorized to send messages to the specified chat ID.
