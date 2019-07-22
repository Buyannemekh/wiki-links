# UpdatePages 
I completed this project in three weeks as a Data Engineering fellow at [Insight Data Engineering Fellows Program](https://www.insightdataengineering.com/) in NYC, June 2019.

---

## Project Summary:
Today we’re living in the “Content Marketing Boom” with more and more businesses are starting up their own blogs. Particularly with tech related blogs, their credibility relies on how up-to-date the information is with rapidly changing technologies and tools. Therefore, it's important to maintain up-to-date articles to be reliable source for readers.

In this project, I created a dashboard to identify pages that are needed to be updated yet popular within the website. Popular meaning the page has cited many times internally and other pages in the blog used it as a source via a hyperlink. 

To achieve this, I used Wikipedia data dump, which is publicly available and met the goal of my project statement. Since Wikipedia page editing is purely volunteer service, some pages can remain outdated. Though Wikipedia has a page on [Articles in need of updating](https://en.wikipedia.org/wiki/Category:Wikipedia_articles_in_need_of_updating), it is hard to keep track of all the pages as data gets accumulated over time.

UpdatePages provides a dashboard for analyzing site-wide current updates on main pages of English Wikipedia. I analyze 45 million Wikipedia pages in zipped XML format, process them in Spark, and store in PostgreSQL. 
[Presentation Slides](http://bit.ly/chrissyslides)

## Data Set:
There is a way to download [wiki dumps](https://dumps.wikimedia.org/) for any project/language, the data is from early 2009. To access the latest versions of all Wikipedia page, go to this page and download files with the prefix "enwiki-latest-pages-meta-current"[1-27]. Wikipedia publishes the full site in 27 parts. Wikipedia offers other options for accessing their data, see a full description [here](https://en.wikipedia.org/wiki/Wikipedia:Database_download)

## Data Pipeline:
![alt text](https://github.com/Buyannemekh/wiki-links/blob/master/img/pipeline-0.png)

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
You provide a row tag of your xml files to treat as a row to parse your file using Spark-XML library and each record under a tag should be read by a single Java Virtual Machine (JVM). Because of this requirement, I have encountered JVM out of memory error due to uneven text information partitioned into my spark machines when some Wikipedia articles are relatively large compare to others. However, I was able to read all the meta data by tuning the {worker, driver} node as well as EC2 instance type. 

After reading the 45 million pages of meta data, I preprocessed and cleaned it by filtering out only original articles by its namespace. 

Another challenge I faced was quantifying the number of incoming links, which I used as a ranking metrics when showing the outdated articles, of each page. Each hyperlinks in each page were extracted using Regex in UDF, then I created the following information in the following DB structure: 

| ID     | Title | Timestamp | Hyperlinks |
|:-------|:------|:------|:------|
| 1       | "X"| 2019 |[“Y”, “Z”]|
| 2 | "Y" |2018|[“X”, “Z”]|

Then the number of incoming links to each page was done by doing data aggregation in spark. 

| ID     | Title | Timestamp | Hyperlinks | Incoming links |
|:-------|:------|:------|:------|:------|
| 1       | "X"| 2019 |[“Y”, “Z”]|“Y”|
| 2 | "Y" |2018|[“X”, “Z”]|“X”|


## Demo
[Dash UI Demo](http://www.wikilinks.dev) 

[YouTube Demo](https://www.youtube.com/watch?v=kWVvCDV_RKo)
