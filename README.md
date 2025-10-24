Nifty Minute Analytics
======================

![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg?style=for-the-badge&logo=python&logoColor=ffdd54)
![PySpark](https://img.shields.io/badge/PySpark-ETL-orange.svg?style=for-the-badge&logo=python&logoColor=ffdd54)
![Plotly](https://img.shields.io/badge/Plotly-Visualization-blueviolet.svg?style=for-the-badge&logo=plotor=white)
![Pandas](https://img.shields.io/badge/Pandas-DataFrame-black.svg?style=for-the-badge&logo=pandas&logoe)
![License](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge&logo=&logoColor=ffdd54)


MarketMinute Analytics is a Spark-powered data analysis project designed to uncover time-based insights from minute-level financial market data. It focuses on identifying intraday price trends, volatility patterns, and trading behaviors using PySpark for scalable data processing and Plotly for interactive visualizations.

--------

Features
--------
- Minute-Level Data Analysis: Processes high-frequency market data to extract hourly and daily trends.
- Volatility Insights: Calculates and visualizes volatility to identify high-risk trading periods.
- Interactive Visualizations: Uses Plotly to generate dynamic charts including line plots, heatmaps, histograms, and box plots.
- Custom Date Filtering: Analyze specific trading days to explore intraday price movements.

Key Analysis
------------
1. Average Close Price per Hour: Understand how prices behave across different hours of the trading day.
2. Maximum Volatility per Day: Identify the most volatile trading days for risk assessment.
3. Top 5 Volatile Days: Highlight days with extreme market movements.
4. Hourly Price Trend for Selected Date: Zoom into specific dates to analyze intraday price behavior.
5. Daily Average Close Trend: Track market performance over time.
6. Heatmap of Hourly Volatility: Visualize volatility patterns across hours and days.

Technologies Used
-----------------
- Apache Spark – Distributed data processing
- PySpark – Python API for Spark
- Plotly – Interactive data visualization
- Pandas – Data manipulation for plotting

How to Run
----------
1. Ensure you have Spark and required Python packages installed.
2. Place the CSV or Excel file in the project directory.
3. Run the script:
   python market_minute_analysis.py

## Output Example

The following visualizations were created using Plotly. These plots provide insights into asset price behavior across different time scales:

### 1. Daily Average Close Price Over Time
- **Description**: Line graph showing the daily average close price from 2015 to 2024.
- **Insight**: Reveals a general upward trend with periodic fluctuations, indicating long-term growth with market variability.

![Result Image1](/outputs/output1.png)

### 2. Hourly Price Trend on 2025-07-25
- **Description**: Line graph depicting hourly average close prices for a single trading day.
- **Insight**: Shows a downward trend throughout the day, suggesting bearish sentiment or intraday selling pressure.

![Result Image2](/outputs/output2.png)

### 3. Heatmap of Hourly Volatility Across Days
- **Description**: Heatmap visualizing hourly volatility across multiple days.
- **Insight**: Highlights periods of high and low volatility, useful for identifying active trading hours and risk zones.

![Result Image3](/outputs/output3.png)

Contact
-------
For questions or collaboration, feel free to reach out via GitHub Issues.

