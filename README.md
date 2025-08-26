Nifty Minute Analytics
=======================

MarketMinute Analytics is a Spark-powered data analysis project designed to uncover time-based insights from minute-level financial market data. It focuses on identifying intraday price trends, volatility patterns, and trading behaviors using PySpark for scalable data processing and Plotly for interactive visualizations.

Features
--------
- Minute-Level Data Analysis: Processes high-frequency market data to extract hourly and daily trends.
- Volatility Insights: Calculates and visualizes volatility to identify high-risk trading periods.
- Interactive Visualizations: Uses Plotly to generate dynamic charts including line plots, heatmaps, histograms, and box plots.
- Custom Date Filtering: Analyze specific trading days to explore intraday price movements.

Key Analyses
------------
1. Average Close Price per Hour: Understand how prices behave across different hours of the trading day.
2. Maximum Volatility per Day: Identify the most volatile trading days for risk assessment.
3. Top 5 Volatile Days: Highlight days with extreme market movements.
4. Hourly Price Trend for Selected Date: Zoom into specific dates to analyze intraday price behavior.
5. Daily Average Close Trend: Track market performance over time.
6. Heatmap of Hourly Volatility: Visualize volatility patterns across hours and days.
7. Volatility Distribution: Explore the spread and frequency of volatility values.
8. Box Plot of Close Prices by Hour: Examine price variation and outliers across trading hours.

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

Contact
-------
For questions or collaboration, feel free to reach out via GitHub Issues.
