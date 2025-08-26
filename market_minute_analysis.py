from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, avg, max
import plotly.express as px

# Start Spark session
spark = SparkSession.builder.appName("MarketMinute_Enhanced_Analysis").getOrCreate()

# Load CSV
df = spark.read.csv("NIFTY 50_Minute_Data.csv", header=True, inferSchema=True)

# Parse timestamp correctly
df = df.withColumn("timestamp", to_timestamp(col("date"), "dd-MM-yyyy HH:mm"))
df = df.withColumn("hour", hour(col("timestamp")))
df = df.withColumn("date_only", col("timestamp").cast("date"))
df = df.withColumn("volatility", col("high") - col("low"))

# Analysis 1: Average close price per hour
avg_close_per_hour = df.groupBy("hour").agg(avg("close").alias("avg_close")).orderBy("hour")

# Analysis 2: Maximum volatility per day
max_volatility_per_day = df.groupBy("date_only").agg(max("volatility").alias("max_volatility")).orderBy("max_volatility", ascending=False)

# Analysis 3: Top 5 most volatile trading days
top_5_volatile_days = max_volatility_per_day.limit(5)

# Analysis 4: Hourly price trend for a selected date
selected_date = "2025-07-25"
hourly_trend = df.filter(col("date_only") == selected_date).groupBy("hour").agg(avg("close").alias("avg_close")).orderBy("hour")

# Analysis 5: Daily average close price trend
daily_avg_close = df.groupBy("date_only").agg(avg("close").alias("daily_avg_close")).orderBy("date_only")

# Analysis 6: Heatmap of hourly volatility across days
hourly_volatility = df.groupBy("date_only", "hour").agg(avg("volatility").alias("avg_volatility"))

# Analysis 7: Distribution of volatility values
volatility_values = df.select("volatility")

# Analysis 8: Box plot of close prices by hour
box_close_by_hour = df.select("hour", "close")

# Convert to Pandas for Plotly
hourly_trend_pd = hourly_trend.toPandas()
daily_avg_close_pd = daily_avg_close.toPandas()
hourly_volatility_pd = hourly_volatility.toPandas()

# Plot 2: Hourly trend for selected date
px.line(hourly_trend_pd, x="hour", y="avg_close", title=f"Hourly Price Trend on {selected_date}").show()

# Plot 3: Daily average close price trend
px.line(daily_avg_close_pd, x="date_only", y="daily_avg_close", title="Daily Average Close Price Over Time").show()

# Plot 4: Heatmap of hourly volatility
heatmap_data = hourly_volatility_pd.pivot(index="hour", columns="date_only", values="avg_volatility")
px.imshow(heatmap_data, aspect="auto", title="Heatmap of Hourly Volatility Across Days").show()

