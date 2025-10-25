# HDBLens: Hybrid Database HDB Resale Analytics🏘️

HDBLens is a web platform designed to provide insightful analytics on the Singapore HDB (Housing & Development Board) resale market. It uniquely combines official resale transaction data with community-generated reviews to offer a holistic view of the housing market, demonstrating a hybrid database approach that leverages both relational and non-relational databases.

## High-Level Overview

This project was developed with a focus on designing and developing a database application that makes use of both a relational database (PostgreSQL) and a non-relational database (MongoDB).

-   **Relational Database (PostgreSQL):** Serves as the system of record for structured, official HDB resale data. It enforces data integrity and supports complex analytical queries.
-   **Non-Relational Database (MongoDB):** Used to store semi-structured data, such as user-generated reviews and sentiment, providing flexibility and scalability for this type of content.

### Key Features

-   **Interactive Analytics Dashboard:** Explore resale price trends, transaction volumes, and property distributions with a rich, filterable interface.
-   **Hybrid Data Insights:** Gain unique insights from features that combine quantitative market data (PostgreSQL) with qualitative user sentiment (MongoDB).
-   **User Authentication and Personalization:** Register for an account to access personalized features like a property watchlist.
-   **Community Reviews:** Read, write, and manage reviews for different HDB towns.
-   **Town-Specific Deep Dives:** Get a detailed profile of any town, including both price analytics and the latest community reviews.

### Technology Stack

-   **Frontend:** Streamlit
-   **Backend:** Python, Pandas, Plotly Express
-   **Databases:** PostgreSQL, MongoDB
-   **Database Interaction:** SQLAlchemy (for PostgreSQL), Pymongo (for MongoDB)

## Getting Started

Follow these instructions to get a local copy of the project up and running.

### Prerequisites

-   Python 3.9+
-   PostgreSQL server
-   MongoDB server

### Installation & Setup

1.  **Create and activate a virtual environment:**
    ```sh
    # For Windows
    python -m venv venv
    .\venv\Scripts\activate

    # For macOS/Linux
    python3 -m venv venv
    source venv/bin/activate
    ```

2.  **Clone the repository:**
    ```sh
    git clone https://github.com/jeregjj/HDBLens.git
    ```

3.  **Install the required dependencies:**
    ```sh
    pip install -r requirements.txt

    # Alternative line
    python -m pip install -r requirements.txt
    ```

4.  **Set up environment variables:**
    Create a file named `.env` in the root of the project directory and add the following, replacing the placeholder values with your actual database connection details:

    ```env
    # PostgreSQL Connection String
    SQL_DSN="postgresql://user:password@host:port/database"

    # MongoDB Connection Details
    MONGO_URI="mongodb://user:password@host:port/"
    MONGO_DB_NAME="hdblens"
    MONGO_COLLECTION="town_reviews"
    MONGO_META_COLLECTION="meta"
    ```

5.  **Run the application:**
    ```sh
    streamlit run app.py
    ```
    The application will automatically create the necessary database tables and load the initial data on the first run.

## Program Features in Detail

### Analytics Dashboard

The core of HDBLens is its powerful analytics dashboard. Users can:

-   **Filter Data:** Dynamically filter HDB resale transactions by town, flat type, date range, and even advanced metrics like price per square meter.
-   **Visualize Trends:** Interactive charts—including line graphs for time-series analysis, bar charts for volume, histograms for distribution, and box plots for categorical comparison—are generated on the fly based on user selections.
-   **Key Performance Indicators (KPIs):** At-a-glance metrics provide a quick summary of the data, such as total transactions, median price, and average price.

### Hybrid Data Model

HDBLens demonstrates the power of a hybrid database architecture:

-   **PostgreSQL** stores the normalized and structured HDB resale data, ensuring data integrity and providing a solid foundation for analytical queries.
-   **MongoDB** is used for its flexible schema, making it ideal for storing user-generated content like reviews, which can evolve over time.
-   The `hybrid_queries.py` module contains functions that intelligently query both databases and join the data in the application layer. A prime example is the "Affordable & Well-Rated Towns" feature on the home page, which ranks towns based on a hybrid score that considers both median resale price (from PostgreSQL) and average user rating (from MongoDB).

### User Authentication & Profile Management

-   **Secure Registration & Login:** Users can create an account and log in securely. Passwords are encrypted using `bcrypt` before being stored.
-   **Profile Page:** Logged-in users have access to a profile page where they can update their email address, reset their password, and manage their account.

### Watchlist

A personalized feature for logged-in users:

-   **Add & Remove:** Users can browse through HDB resale listings and add any that interest them to a personal watchlist.
-   **Centralized View:** The watchlist page provides a dedicated view of all saved listings, allowing users to track properties they are interested in.

### Community Reviews

-   **Full CRUD Functionality:** Users can create, read, update, and delete their own reviews for any HDB town.
-   **Sentiment Analysis:** The reviews contribute to the overall sentiment analysis of each town, providing a qualitative layer of data that complements the quantitative market data.

### Town Sentiment Analysis

The "Town Sentiment" page allows users to perform a deep dive into a specific town. By selecting a town, users can view a detailed profile that includes:

-   **Price Analytics:** Median, 25th, and 75th percentile prices for the last 12 months.
-   **Community Sentiment:** The average user rating and total number of reviews for the town.
-   **Latest Reviews:** A feed of the most recent community reviews for that town.
