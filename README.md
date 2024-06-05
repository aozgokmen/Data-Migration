# Data-Migration

Data-Migration is a project designed to facilitate data migration and transformation between different databases. This project is tailored to handle migrations between PostgreSQL databases and from PostgreSQL to Google BigQuery.

## Features

- Data migration between PostgreSQL databases
- Data migration from PostgreSQL to Google BigQuery
- Data validation and cleaning
- Easy configuration and extensibility

## Installation

To set up this project on your local machine, follow these steps:

1. Clone this repository:

    ```sh
    git clone https://github.com/aozgokmen/Data-Migration.git
    ```

2. Navigate to the project directory:

    ```sh
    cd Data-Migration
    ```

3. Install the required dependencies. This project uses Python and the required packages are listed in the `requirements.txt` file:

    ```sh
    pip install -r requirements.txt
    ```

## Usage

### PostgreSQL to PostgreSQL Migration

To migrate data from one PostgreSQL database to another:

```sh
python migrate.py --source postgres://user:password@source_host:source_port/source_db --destination postgres://user:password@destination_host:destination_port/destination_db
