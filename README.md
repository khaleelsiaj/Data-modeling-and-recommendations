# Retail Data ETL and Recommendation System

This project demonstrates a complete data pipeline for retail data, integrating ETL (Extract, Transform, Load) processes with a recommendation system. It highlights essential data engineering skills using **Python**, **SQL**, and **Bash**, alongside a basic item-based collaborative filtering recommendation model. The pipeline automates database creation, processes a large retail dataset (~500,000 rows), and generates personalized product recommendations based on customer purchase history.

## Table of Contents
- [Project Overview](#project-overview)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [How to Run](#how-to-run)
- [Contributing](#contributing)
- [License](#license)

## Project Overview
This project automates the setup of a PostgreSQL database, performs ETL on a retail dataset, and provides product recommendations. It includes:
- **Database Automation**: Creates a PostgreSQL database and tables using Python.
- **ETL Pipeline**: Reads, cleans, and loads a large dataset into the database.
- **Recommendation System**: Suggests items to customers based on past purchases using collaborative filtering.

The dataset (~500,000 rows) is processed efficiently, making this a practical example of data engineering and basic machine learning in action.

## Features
- **Automated Database Setup**: Uses Python's `psycopg2` to create a PostgreSQL database and tables.
- **Efficient ETL Pipeline**: Handles data extraction, cleaning, and loading for large datasets.
- **Recommendation Engine**: Implements item-based collaborative filtering with cosine similarity.
- **Logging**: Includes detailed logs for monitoring and debugging.

## Technologies Used
- **Python**: Core language for scripting and logic.
- **PostgreSQL**: Database system for storing retail data.
- **Bash**: Script to orchestrate the pipeline.
- **Key Libraries**:
  - `psycopg2`: PostgreSQL integration with Python.
  - `pandas`: Data manipulation and preprocessing.
  - `scikit-learn`: Cosine similarity for recommendations.
  - `logging`: Pipeline and model monitoring.
  - `argparse`: Command-line argument parsing.

## Project Structure
- **`db_setup.py`**: Sets up the database and tables.
- **`etl_pipeline.py`**: Executes the ETL process.
- **`recommendation_model.py`**: Generates customer recommendations.
- **`run_pipeline.sh`**: Bash script to run the pipeline.
- **`logs/`**: Stores log files for debugging.
- **`data/`**: Contains the dataset (e.g., `Online Retail.csv`).
- **`tests/`**: Unit tests (if applicable).

## Installation
Follow these steps to set up the project locally:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/your-repo-name.git
   cd your-repo-name
   ```

2. **Create a Virtual Environment**:
   ```bash
   python3 -m venv venv
   ```

3. **Activate the Virtual Environment**:
   ```bash
   source venv/bin/activate
   ```

4. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

5. **Install PostgreSQL**: Ensure PostgreSQL is installed and running on your system. Download it from postgresql.org.

## Usage
Configure Environment Variables
Create a .env file in the project root with:
```text
DEFAULT_DB=postgres
DBNAME=retail_db
USER=postgres
PASSWORD=your_postgres_password
HOST=localhost
```
Replace your_postgres_password with your PostgreSQL password.

Run the ETL Pipeline
Execute the Bash script to set up the database and load data:
```bash
./run_pipeline.sh
```

Generate Recommendations
Run the recommendation model with a customer ID:
```bash
python3 recommendation_model.py --customer_id <CUSTOMER_ID>
```
Optionally specify the number of recommendations:
```bash
python3 recommendation_model.py --customer_id <CUSTOMER_ID> --top_n 5
```

## How to Run
Here’s a step-by-step guide to running the project:

Prepare the Environment
Ensure PostgreSQL is running.

Activate the virtual environment:
```bash
source venv/bin/activate
```

Execute the Pipeline
Run the Bash script to create the database and load data:
```bash
./run_pipeline.sh
```

Get Recommendations
Run the recommendation script with a customer ID (e.g., 12346):
```bash
python3 recommendation_model.py --customer_id 12346
```
Check the logs/recommendation_model.log file for output.

## Contributing
Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a feature branch:
   ```bash
   git checkout -b feature-name
   ```
3. Commit your changes:
   ```bash
   git commit -m "Add feature"
   ```
4. Push to the branch:
   ```bash
   git push origin feature-name
   ```
5. Open a pull request with a clear description.

For issues or suggestions, please open an issue on GitHub.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
