# UpdatePages 
This is a project carried out during the seven-week [Insight Data Engineering Fellows Program](https://www.insightdataengineering.com/) which helps recent grads and experienced software engineers learn the latest open source technologies by building a data platform to handle large, real-time datasets.

---

## Project Summary:
This project helps Wikipedia to identify outdated articles. 

## Data Set:
There is a way to download [wiki dumps](https://dumps.wikimedia.org/) for any project/language, the data is from early 2009. To access the latest versions of all Wikipedia page, go to this page and download files with the prefix "enwiki-latest-pages-meta-current"[1-27]. Wikipedia publishes the full site in 27 parts. Wikipedia offers other options for accessing their data, see a full description [here](https://en.wikipedia.org/wiki/Wikipedia:Database_download)

## Data Pipeline:
![alt text](https://github.com/Buyannemekh/wiki-links/blob/development/img/pipeline.png)

UpdatePages is a batch processing pipeline over a large volume of data.

I downloaded all current pages on the English version of Wikipedia to an S3 bucket, which were in the format of bz2 zipped XMLs, using shell script. Spark [Databricks Spark XML package](https://github.com/databricks/spark-xml) was used to read and parse these input files into a dataframe. Data cleaning and processing operations were done using Spark and final tables were written in PostgreSQL. Finally, an interactive website was created with Dash Plotly, which reads query from PostgreSQL database. 

| Directory                       | Description of Contents
|:--------------------------------|:---------------------------------------- |
| `src/dash/*`                    | HTML and CSS that queries DB and builds the UI |
| `src/batch_pocess/parse_xml.py` | Reads from S3, unzips, parses, and writes into PostgreSQL |
| `src/dataingestion/*`           | Shell script to download data set from Wikipedia datadump |
| `test`                          | Unit test for a smaller dataset |


### Cluster set up
This project used following EC2 nodes to Spark and Hadoop set up
- master node m4.large
- three worker nodes m4.2xlarge 

### Environment 
Install AWS CLI and [Pegasus](https://github.com/InsightDataScience/pegasus), which is Insight's automatic cluster creator. Set the configuration in workers.yml and master.yml (3 workers and 1 master), then use Pegasus commands to spin up the cluster and install Hadoop and Spark. Clone the databricks [XML parsing package](https://github.com/databricks/spark-xml) and follow the setup instructions that they provide. 

| Technology     | Version No.
|:-------------- |:----------- |
| Hadoop       | v2.7.6 |
| Spark | v2.12 |
| spark-xml | v2.12|
| Postgres | v10.6 |



## Project Challenge



