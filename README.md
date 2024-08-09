# Apple Sales Analysis
Sales Analysis for Apple Products Using Databricks to create multiple ETL Pipelines with PySpark.

## Project Overview
This project involves conducting a comprehensive sales analysis for Apple products, leveraging Databricks to create multiple ETL (Extract, Transform, Load) pipelines using PySpark. The objective was to derive actionable insights from transactional data, focusing on customer purchasing behavior, particularly around the sales of iPhones and AirPods. The analysis addressed the following key questions:
1. Which customers bought AirPods immediately after purchasing an iPhone?
2. Which customers purchased only AirPods and an iPhone without buying any other products?
3. What was the average delay in days between the purchase of an iPhone and AirPods?

## Tools and Technologies
- **Apache Spark:** Used for distributed data processing, enabling efficient handling of large datasets.
- **Databricks:** Provided the collaborative environment and computational power necessary for developing, testing, and deploying the ETL pipelines.
- **PySpark:** Facilitated the implementation of Spark's capabilities in Python, streamlining the ETL processes.
- **MySQL:** Utilized as a relational database for storing and managing transactional data.

## Key Skills and Concepts Acquired

- **Low-Level Design (LLD) of Codebases:** Developed a strong understanding of the principles behind low-level design, particularly in structuring code using factory patterns to ensure modularity, scalability, and maintainability.

- **ETL Pipeline Development**: Mastered the use of separate extractor, transformer, and loader classes, following best practices for building robust and reusable ETL pipelines. This approach ensured a clean separation of concerns and improved the code's readability and efficiency.

- **Spark Job Optimization:** Gained in-depth knowledge of Spark's execution model, including the concepts of jobs, stages, and tasks. This understanding was critical in optimizing the performance of Spark jobs by minimizing shuffles, leveraging narrow and wide transformations effectively, and applying techniques such as broadcast joins.

- **Cluster Management and Delta Tables:** Gained proficiency in setting up and managing clusters on Databricks, along with creating and utilizing Delta Tables for optimized data storage and retrieval.

- **Advanced Spark Concepts:** Explored advanced Spark techniques, including partitioning and data distribution strategies, to improve query performance and ensure efficient resource utilization across the cluster.
