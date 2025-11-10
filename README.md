# PySpark-Data-Processing

# Flight Delays Data Analysis with Pyspark and Databricks

## Dataset Description and Source
The dataset used in this project comes from the Databricks sample dataset, specifically: databricks-datasets/flights/departuredelays.csv

This dataset contains flight depature information, including:
date: integer (not listed as a real date format)
delay: integer (depature delay in minutes)
distance: integer (distance of flight in miles)
origin: string (airport of origin)
destination: string (destination airport)

The dataset was duplicated to simulate a large-scale dataset (~1 GB, ~14 million rows) to demonstrate Spark’s distributed processing capabilities and performance optimizations on big data.

## Project Overview

The goal of this project was to build and optimize a PySpark data processing pipeline that demonstrates:

- Lazy vs eager evaluation
- Filtering, transformations, and aggregations at scale
- Spark SQL interoperability
- Performance optimization

## Performance Analysis

### How Spark Optimized the Query
Spark's catalyst optimizer automatically restructured query plans to minimize computation. For example, when multiple `.filter()` and `.select()` transformations were chained together, Spark combined them into a single physical plan, reducing the number of scans over the dataset. The optimizer also applied predicate pushdowns, ensuring filters such as 'origin = 'SFO'' and 'delay > 60' were executed as early as possible in the pipeline; this significantly reduced the amount of data that was shuffled and processed later on

### Filter Pushdown and Column Pruning
After inspecting the output of my `.explain(True)` statements, Spark’s logical plan confirmed filter pushdown and column pruning optimizations:
- Filters on `origin`, `delay`, and `distance` were pushed down to the file scan stage.  
- Only necessary columns (`date`, `origin`, `destination`, `delay`, `distance`) were loaded into memory, avoiding unnecessary I/O.

These optimizations reduced the dataset size before grouping and aggregation, leading to faster shuffle operations and lower memory usage.

### Identified Bottlenecks and Optimizations
Initial performance bottlenecks were observed during `groupBy` aggregations due to excessive data shuffling.  
To address this:
- Filter ordering was applied early in the pipeline (`origin → delay → distance`) to minimize intermediate data size.  
- Repartitioning by `delay_category` balanced the workload across executors.  

## Key Findings from Data Analysis
- Flights with a destination of MIA had the highest average delay of about 180 minutes, with a max delay of 740 minutes
- There were no flights in the dataset that constituted a "Minor or No Delay)
- Most delayed flights clustered around a few major destinations, suggesting potential scheduling or congestion patterns.  
- After optimization, Spark efficiently processed the 1 GB dataset in seconds using distributed parallelism.

  
