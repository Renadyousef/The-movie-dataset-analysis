#we did this in our main spark session and extracted results but then for some reason, the session wont start anymore so we kept the code of the first session
#since it all ran and shoes output but the visual and added the visual here as supplementary to our results

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import when


spark = SparkSession.builder.appName("Local Spark in Jupyter").getOrCreate()




movie_stats = movie_stats.withColumn('label', when(movie_stats['ratings_count'] > 1000, 1).otherwise(0))


data_pandas = movie_stats.select("movieId", "ratings_count", "label").toPandas()


high_rated_movies = data_pandas[data_pandas['label'] == 1]
low_rated_movies = data_pandas[data_pandas['label'] == 0]

# Create scatter plot
plt.figure(figsize=(10,6))

# Plot low-rated movies with blue dots
plt.scatter(low_rated_movies['movieId'], low_rated_movies['ratings_count'], color='blue', label='Low Rated (<1000 ratings)', alpha=0.7)

# Plot high-rated movies with red dots
plt.scatter(high_rated_movies['movieId'], high_rated_movies['ratings_count'], color='red', label='High Rated (>1000 ratings)', alpha=0.7)

# Add title and labels
plt.title('Scatter Plot of Movie Ratings Frequency')
plt.xlabel('Movie ID')
plt.ylabel('Rating Count')
