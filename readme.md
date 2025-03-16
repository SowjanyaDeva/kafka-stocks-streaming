# 📊 Real-Time Stock Price Streaming with Kafka & Snowflake  

## 🚀 **Overview**  
This project implements a **real-time stock price streaming pipeline** using **Kafka, Snowflake, and Python**. It **fetches stock data from the Alpha Vantage API**, streams it through Kafka, and **ingests the data into Snowflake** for further analysis.  

## **🛠 Tech Stack**
- **Kafka** – Real-time data streaming
- **Python** – Data fetching & processing
- **Snowflake** – Cloud-based data warehouse
- **Airflow (Optional)** – Workflow automation
- **Alpha Vantage API** – Stock data source  

## **🔹 Features**
✅ **Fetch & Stream Stock Data:** Automates stock price retrieval for multiple tickers (**AAPL, MSFT, AMZN, GOOGL, META**).  
✅ **Kafka Producer & Consumer:** Streams and processes data in real-time using Kafka.  
✅ **Snowflake Integration:** Stores and processes stock data efficiently.  
✅ **Automated ETL Pipelines:** Supports **Airflow-style orchestration** for seamless automation.  
✅ **Graceful Shutdown & Fault Tolerance:** Handles errors and auto-restarts in case of failures.  

## **📂 Project Structure**
![Untitled Diagram1 drawio](https://github.com/user-attachments/assets/0da55a59-1d1e-4888-a26c-56c7a8c19da1)
