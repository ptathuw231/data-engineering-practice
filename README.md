-
**Thành viên nhóm:**
- Phạm Thị Anh Thư - 23643081
- Trần Nhật Tiến - 23673681
---

## Data Engineering Practice Problems

One of the main obstacles of Data Engineering is the large
and varied technical skills that can be required on a 
day-to-day basis.

*** Note - If you email a link to your GitHub repo with all the completed
exercises, I will send you back a free copy of my ebook Introduction to Data Engineering. ***

This aim of this repository is to help you develop and 
learn those skills. Generally, here are the high level
topics that these practice problems will cover.

- Python data processing.
- csv, flat-file, parquet, json, etc.
- SQL database table design.
- Python + Postgres, data ingestion and retrieval.
- PySpark
- Data cleansing / dirty data.

### How to work on the problems.
You will need two things to work effectively on most all
of these problems. 
- `Docker`
- `docker-compose`

All the tools and technologies you need will be packaged
  into the `dockerfile` for each exercise.

For each exercise you will need to `cd` into that folder and
run the `docker build` command, that command will be listed in
the `README` for each exercise, follow those instructions.

### Beginner Exercises

#### Exercise 1 - Downloading files.
The [first exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-1) tests your ability to download a number of files
from an `HTTP` source and unzip them, storing them locally with `Python`.
`cd Exercises/Exercise-1` and see `README` in that location for instructions.

![image](https://github.com/user-attachments/assets/792be522-45b3-411a-899f-369f43181966)
![image](https://github.com/user-attachments/assets/264e35f5-ac79-4c51-bbfe-6e5579981c38)
![image](https://github.com/user-attachments/assets/d4837a00-d412-4e3d-a004-e3b557d02ae0)
![image](https://github.com/user-attachments/assets/be248502-369b-4215-bfba-894f1cc622af)
![image](https://github.com/user-attachments/assets/d5b900a5-0493-42ca-9ed4-6e16ba38da9e)
![image](https://github.com/user-attachments/assets/02be479f-3a09-42de-a40d-d022b5ea4a4b)
![image](https://github.com/user-attachments/assets/fb1a5a5f-214c-4590-ace3-5d39be8f3d3f)



#### Exercise 2 - Web Scraping + Downloading + Pandas
The [second exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-2) 
tests your ability perform web scraping, build uris, download files, and use Pandas to
do some simple cumulative actions.
`cd Exercises/Exercise-2` and see `README` in that location for instructions.
![image](https://github.com/user-attachments/assets/40b70aa4-c743-4ca2-8c2e-2d23a26b7ddd)
![image](https://github.com/user-attachments/assets/b7492cb5-c24e-4294-bfd0-33d5b7790250)



#### Exercise 3 - Boto3 AWS + s3 + Python.
The [third exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-3) tests a few skills.
This time we  will be using a popular `aws` package called `boto3` to try to perform a multi-step
actions to download some open source `s3` data files.
`cd Exercises/Exercise-3` and see `README` in that location for instructions.


#### Exercise 4 - Convert JSON to CSV + Ragged Directories.
The [fourth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-4) 
focuses more file types `json` and `csv`, and working with them in `Python`.
You will have to traverse a ragged directory structure, finding any `json` files
and converting them to `csv`.
![image](https://github.com/user-attachments/assets/cf9c4de5-2bcb-416a-8a7c-fcdaa871759e)
![image](https://github.com/user-attachments/assets/c000b830-a67c-4b5d-a975-84c79e46a307)
![image](https://github.com/user-attachments/assets/ed1748d0-c796-4402-9714-3a944feba22c)
![image](https://github.com/user-attachments/assets/f6cbb4b4-5f1c-4d2f-8458-de593034531c)
![image](https://github.com/user-attachments/assets/b4fd5b51-8cde-4933-a65a-56102ec1413e)
![image](https://github.com/user-attachments/assets/91f3b46b-bba7-4dca-afe7-ada01065219f)
![image](https://github.com/user-attachments/assets/979318ef-431e-4e2e-9ce3-45d0d82c5318)
![image](https://github.com/user-attachments/assets/85bd802f-95b5-409e-ab32-defaf558fe08)
![image](https://github.com/user-attachments/assets/d5542882-98a9-4f5f-9c7f-c2219c7362bb)
![image](https://github.com/user-attachments/assets/b38002ec-9003-4699-8bec-9fbb804151c9)
#### Exercise 5 - Data Modeling for Postgres + Python.
The [fifth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-5) 
is going to be a little different than the rest. In this problem you will be given a number of
`csv` files. You must create a data model / schema to hold these data sets, including indexes,
then create all the tables inside `Postgres` by connecting to the database with `Python`.
![image](https://github.com/user-attachments/assets/0940a429-7df2-44d5-9000-ccd418f3ba46)
![image](https://github.com/user-attachments/assets/0bb57863-2a3e-49b1-8506-608809b6eb3a)
![image](https://github.com/user-attachments/assets/5b83de03-f9dc-4ea6-beb8-ca95865af59a)
![image](https://github.com/user-attachments/assets/9ddd7713-955e-4147-81d1-9a080d1d29f8)
![image](https://github.com/user-attachments/assets/c5e70b2e-f04b-4b62-839a-77642be18f9c)

### Intermediate Exercises

#### Exercise 6 - Ingestion and Aggregation with PySpark.
The [sixth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-6) 
Is going to step it up a little and move onto more popular tools. In this exercise we are going
to load some files using `PySpark` and then be asked to do some basic aggregation.
Best of luck!
![image](https://github.com/user-attachments/assets/1959baf4-55cf-45a5-87ee-b1c96f92f53c)
![image](https://github.com/user-attachments/assets/f83e20b3-8ca0-4d75-a4c4-9e8b6da4125d)
![image](https://github.com/user-attachments/assets/cf0cc851-d94e-4201-af43-2ad4584a22bf)
![image](https://github.com/user-attachments/assets/a28b9d41-fc69-4b53-9eba-11856e84a7fd)

#### Exercise 7 - Using Various PySpark Functions
The [seventh exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-7) 
Taking a page out of the previous exercise, this one is focus on using a few of the
more common build in PySpark functions `pyspark.sql.functions` and applying their
usage to real-life problems.

Many times to solve simple problems we have to find and use multiple functions available
from libraries. This will test your ability to do that.

#### Exercise 8 - Using DuckDB for Analytics and Transforms.
The [eighth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-8) 
Using new tools is imperative to growing as a Data Engineer. DuckDB is one of those new tools. In this
exercise you will have to complete a number of analytical and transformation tasks using DuckDB. This
will require an understanding of the functions and documenation of DuckDB.

#### Exercise 9 - Using Polars lazy computation.
The [ninth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-9) 
Polars is a new Rust based tool with a wonderful Python package that has taken Data Engineering by
storm. It's better than Pandas because it has both SQL Context and supports Lazy evalutation 
for larger than memory data sets! Show your Lazy skills!


### Advanced Exercises

#### Exercise 10 - Data Quality with Great Expectations
The [tenth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-10) 
This exercise is to help you learn Data Quality, specifically a tool called Great Expectations. You will
be given an existing datasets in CSV format, as well as an existing pipeline. There is a data quality issue 
and you will be asked to implement some Data Quality checks to catch some of these issues.
