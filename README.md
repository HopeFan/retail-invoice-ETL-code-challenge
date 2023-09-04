# BI Engineering Homework Assignment
Thank you for taking the time to tackle this assignment. We've tried to create a scenario that reflects the sorts of challenges you'll be working on here. **It is important that you read the context and instructions that follow**. Enjoy!

## About Data & AI at XXX
Data & AI at XXX is at an inflection point that reflects XXX’s growth over the last century supporting trade industries and improving the lives of our customers and people every day. We are known for an unparalleled, human understanding of our customers — and we have a clear, long-term vision for becoming trade’s most valuable partner, helping them succeed in a digital world.

## About Our Branch Operations
XXX’s branch operations are at the heart of our customer experience. Our branch teams regularly engage in detailed customer relationship development meeting cadences with our Trade customers to understand their businesses, and co-develop approaches to helping them succeed — these activities negotiate wide range of topics such as how pricing, logistics, merchandising, product lines and other services can be tailored to help our customers run their businesses more successfully. Our branch teams leverage our customers transaction information through our in-house point of sale system (The XXX System, TRS), as well as tools such as Salesforce Customer 360 and PowerBI reporting to make sense of customers’ interactions with XXX to inform who they should be talking to, for what specific purpose (e.g. marketing, relationship development, operating/commercial activities such as navigating of credit arrangements on occasion required).

## Instructions for this assignment
One area XXX is considering offering enhanced service to our customers is in assisting them with meeting their credit account payment obligations. Majority of XXX customers maintain trading credit accounts with XXX –– that is, trade supplies are invoiced against their respective trade accounts, with customer given 30 days (inclusive) to pay off their invoices.

Currently our branch staff understand intuitively how likely our customers are to pay their invoices on time through examining transactional information and based on their intuition, initiating conversations with customers who may need to discuss working through payment terms in a given period. However, this exercise is mostly manually administered (through branch staff eyeballing TRS debtor histories) and dependent on branch staff’s experience; XXX is considering providing supporting capabilities to help all branches use data to more systematically identify and prioritise customers to speak to about invoice payments.

### The Data
The DBAs who manage TRS have delivered extracts from 4 tables from the production databases:

- **accounts.csv**: Contains account information for our customers
- **skus.csv**: Contains the details about our product lines
- **invoices.csv**: Contains the invoices issued to each account
- **invoice_line_items.csv**: Contains the details of the SKUs sold on each invoice

The data can be found in the `task_one/data_extracts` folder in this repository.

### Task One: Building an ETL pipeline to support our Applied Data Scientists
You've been working closely with the Applied Data Scientists in the team to understand what features might be predictive for late invoice payment. Now, the scientists have requested that a dataset be produced on a regular basis. As part of your response, please add to the repository the code and documentation that:

- **Transforms the raw data into the features required by the scientists**

**Answer** : first I created `data_investigation.py` to become more familiar with the data. Then craeted the ETL data pipeline here `trs_pipeline.py` 

- **Tests your ETL job using the CSV extracts provided**

**Answer** : the pipeline is tested.  we can also impletent some unit tests to test the functions in the pipeline

- **Highlights any intermediate data models you might use to store the data and why**

**Answer** : intermediate tables are highlighted in this DAG chart.


![Drag Racing](DAG.jpg)


There are five intermediate tables:

- **account_invoice_df**: join invoice table with account table
- **sku_invoice_line_items_df**: join sku table with invoice_line_items table
- **join_all**:join account_invoice_df with sku_invoice_line_items_df
- **aggregation_df**: calcuate the reuqired features expeft is_late 
- **is_late_df**: calcuate is_late feature 
- **final_df**: join aggregation_df and is_late_df to have everything in one place for Data Scientists

**Design Choices**

I used:
- PySpark's DataFrame API to read and transform the data. The DataFrame API is a powerful and flexible way to manipulate large-scale datasets in PySpark. It allows you to perform operations on data in a tabular format using SQL-like syntax.

- PySpark's Window function to perform calculations over a group of rows, such as counting the number of invoices within a certain period for each account. This is an efficient way to perform complex aggregations and calculations on large datasets.

- used PySpark's join function to combine multiple tables based on a common column, such as joining the invoice table with the account table. This effectively combines information from different sources and creates a comprehensive view of the data.

- used PySpark's when the function to create conditional statements, such as determining if a payment is late based on the date difference between the payment date and the date issued. This is a useful way to add logic to your data pipeline and perform complex calculations.

**Scaling Considerations**

- One of the primary scaling considerations for PySpark data pipelines is the amount of memory available for processing. As the size of the dataset increases, you may need to allocate more memory to Spark to ensure that it can process the data efficiently.

- Another consideration is the number of partitions Spark uses when processing the data. By default, Spark partitions the data based on the number of cores available on the Spark machine. However, you can also manually adjust the number of partitions to optimize performance based on the size and complexity of the dataset.

- When joining large tables, consider the join strategy used by Spark to ensure that it can handle the join efficiently. Spark supports different join strategies, such as broadcast join, sort-merge join, and shuffle hash join, each with different trade-offs regarding memory usage and performance. You may need to experiment with different strategies to determine the most efficient approach for your data pipeline.

- Finally, consider the size and format of the data when reading and writing it to disk. For example, using a compressed file format such as Parquet or ORC can significantly reduce the storage space needed for large datasets.


### Task Two: System Design
Having written the ETL job to do the data transformation required, we now need to design the overall system architecture that will allow the Data Team to operationalise the ETL job and maintain it as a long-lifed production system. As part of your response, please add to the repository the documentation that:

- **Shows the architecture of a production system that could run your (and similar) ETL jobs, before exposing the resulting datasets to the various consumers.**

Architecture of a production system that could run ETL jobs on a regular basis:
One possible architecture for a production system that could run ETL jobs on a regular basis would be a batch processing architecture. This architecture would involve the following components:

**Source system (TRS)**: The source system stores the data in an AWS Aurora MySQL database. This database will be the source of data for our ETL job.

**ETL system**: This system would include the ETL job that I have already built, along with any additional processing that needs to be done. For example, I may need to perform data validation, data cleansing, or data aggregation. The ETL job can be scheduled to run on a regular basis using a job scheduler such as AWS Glue, which supports serverless 

**Data storage system**: This system would be responsible for storing the resulting datasets in a format that is optimised for querying. Examples of data storage systems include data lakes, data warehouses, and Hadoop clusters. It can be an object store like Amazon S3 or a relational database like Amazon RDS. Storing the data in an object store provides cost savings, while storing the data in a relational database allows for easier querying.

**batch analytics and stream analytics system**: The batch analytics system will process the data stored in the data store and provide the scientists with up-to-date data on a regular basis. This can be achieved using batch processing frameworks like Apache Spark or Apache Flink. Batch analytics will generate and store aggregate features or summary statistics over time for use by the scientists.

**BI system**: The finance and BI teams can query the invoice data stored in the data store using SQL. They can use SQL querying tools like Amazon Athena or Amazon Redshift for querying the data. Using a SQL querying tool allows the finance and BI teams to query the data without needing to know programming languages like Python or Scala.

Here is a high-level diagram of this architecture:

![Drag Racing](high-level-architecture.jpg)

- **Indicates how your architecture would evolve to support near real-time updating of data as the TRS engineering team enables CDC**.

Architecture Evolution for Near Real-Time Updating of Data:

![Drag Racing](realtime-high-level-archiectrue.jpg)

To support near real-time updating of data as the TRS engineering team enables CDC, the production system architecture can be modified as follows:

The architecture includes the following components:

**CDC Events**: The CDC events from the Aurora MySQL database will be placed onto Kafka topics. Kafka is a distributed streaming platform that can be used to build real-time data pipelines and streaming applications.

**Streaming processing system**: The ETL job will be modified to read the CDC events from Kafka and update the data store in near real-time. Streaming ETL can be implemented using frameworks like Apache Kafka Streams or Apache Flink. These frameworks allow for processing of large amounts of data with low latency and high throughput.

**Stream Analytics**: The stream analytics component will process the data stored in the data store and provide the scientists with up-to-date data in near real-time. Stream analytics can be implemented using stream processing frameworks like Apache Kafka Streams or Apache Flink. Stream analytics can generate and store near real-time aggregate features or summary statistics for use by the scientists.

- **Highlights any key architectural decisions made, and explains them**.

Key Architectural Decisions:

Some key architectural decisions made in the proposed production system are:

**Use of Serverless ETL**: The use of serverless ETL allows for cost savings and automatic scaling of resources. AWS Glue can automatically generate ETL code and scale to handle large datasets.

**Use of Object Store or Relational Database**: The decision to store data in an object store or relational database is based on the trade-off between cost and querying ease. Storing

