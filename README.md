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

### Lab 8
![image](https://github.com/user-attachments/assets/552be05e-3fd4-49f9-aea9-a81470abc994)
![image](https://github.com/user-attachments/assets/5cfa2f87-f471-490a-b667-52278e1787fe)
![image](https://github.com/user-attachments/assets/908cf416-e4fd-456b-8184-587e69e0d5c6)
![image](https://github.com/user-attachments/assets/db4c440a-37c6-419d-9234-321f1241d1d8)
![image](https://github.com/user-attachments/assets/5e98250f-e3bc-4c40-9590-d5ebddb771bd)

### Beginner Exercises

#### Exercise 1 - Downloading files.
The [first exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-1) tests your ability to download a number of files
from an `HTTP` source and unzip them, storing them locally with `Python`.
`cd Exercises/Exercise-1` and see `README` in that location for instructions.

Tiến hành chạy lệnh cd data-engineering-practice/Exercises/Exercise-1 để thay đổi đường dẫn thư mục Exercise-1
Tiếp tục thực hiện lệnh: docker build --tag=exercise-1 . để build Docker image Quá trình sẽ mất vài phút
![image](https://github.com/user-attachments/assets/18340206-8dc3-46b2-974d-756c246a3e50)
![image](https://github.com/user-attachments/assets/264e35f5-ac79-4c51-bbfe-6e5579981c38)
![image](https://github.com/user-attachments/assets/d4837a00-d412-4e3d-a004-e3b557d02ae0)
![image](https://github.com/user-attachments/assets/be248502-369b-4215-bfba-894f1cc622af)
![image](https://github.com/user-attachments/assets/d5b900a5-0493-42ca-9ed4-6e16ba38da9e)
![image](https://github.com/user-attachments/assets/02be479f-3a09-42de-a40d-d022b5ea4a4b)
![image](https://github.com/user-attachments/assets/fb1a5a5f-214c-4590-ace3-5d39be8f3d3f)

> ##### Code sử dụng cho main.py
```
import requests
import os
import zipfile
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from pathlib import Path
import unittest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

DOWNLOAD_DIR = "downloads"

def create_download_directory():
    """Create downloads directory if it doesn't exist."""
    Path(DOWNLOAD_DIR).mkdir(exist_ok=True)

def get_filename_from_uri(uri):
    """Extract filename from URI."""
    return uri.split('/')[-1]

def extract_zip(zip_path):
    """Extract CSV from zip file and remove the zip."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)
        os.remove(zip_path)
        logger.info(f"Extracted and removed {zip_path}")
    except zipfile.BadZipFile:
        logger.error(f"Invalid zip file: {zip_path}")
        os.remove(zip_path)

def download_file(uri):
    """Download a single file synchronously."""
    filename = get_filename_from_uri(uri)
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    
    try:
        response = requests.get(uri, stream=True)
        if response.status_code == 200:
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Downloaded {filename}")
            extract_zip(file_path)
        else:
            logger.error(f"Failed to download {uri}: Status {response.status_code}")
    except requests.RequestException as e:
        logger.error(f"Error downloading {uri}: {str(e)}")

async def download_file_async(session, uri):
    """Download a single file asynchronously."""
    filename = get_filename_from_uri(uri)
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    
    try:
        async with session.get(uri) as response:
            if response.status == 200:
                with open(file_path, 'wb') as f:
                    f.write(await response.read())
                logger.info(f"Downloaded {filename} (async)")
                extract_zip(file_path)
            else:
                logger.error(f"Failed to download {uri}: Status {response.status}")
    except aiohttp.ClientError as e:
        logger.error(f"Error downloading {uri}: {str(e)}")

async def download_files_async(uris):
    """Download files asynchronously using aiohttp."""
    async with aiohttp.ClientSession() as session:
        tasks = [download_file_async(session, uri) for uri in uris]
        await asyncio.gather(*tasks)

def download_files_threaded(uris):
    """Download files using ThreadPoolExecutor."""
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(download_file, uris)

def main():
    """Main function to execute the download process."""
    create_download_directory()
    
    # Synchronous download
    logger.info("Starting synchronous downloads")
    for uri in download_uris:
        download_file(uri)
    
    # Threaded download
    logger.info("Starting threaded downloads")
    download_files_threaded(download_uris)
    
    # Async download
    logger.info("Starting async downloads")
    asyncio.run(download_files_async(download_uris))

class TestDownloadFunctions(unittest.TestCase):
    def setUp(self):
        create_download_directory()
    
    def test_get_filename_from_uri(self):
        uri = "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip"
        self.assertEqual(get_filename_from_uri(uri), "Divvy_Trips_2018_Q4.zip")
    
    def test_create_download_directory(self):
        self.assertTrue(os.path.exists(DOWNLOAD_DIR))
    
    def test_invalid_uri(self):
        uri = "https://invalid-url/does_not_exist.zip"
        download_file(uri)
        filename = get_filename_from_uri(uri)
        self.assertFalse(os.path.exists(os.path.join(DOWNLOAD_DIR, filename)))

if __name__ == "__main__":
    main()
```

> Đoạn code trên thực hiện các tác vụ: 
- Tạo thư mục downloads nếu chưa tồn tại

- Tải từng file từ danh sách download\_uris

- Giữ tên gốc của file từ URL

- Giải nén .zip thành .csv

- Xóa file .zip sau khi giải nén

- Bỏ qua URL không hợp lệ (ví dụ: cái Divvy\_Trips\_2220\_Q1.zip không tồn tại)
  
![image](https://github.com/user-attachments/assets/792be522-45b3-411a-899f-369f43181966)
![image](https://github.com/user-attachments/assets/a2dfe656-27e6-4460-b9ee-ce0c6faa38f8)
![image](https://github.com/user-attachments/assets/af012498-0f98-4110-bb6e-17dc7f2a0b72)


#### Exercise 2 - Web Scraping + Downloading + Pandas
The [second exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-2) 
tests your ability perform web scraping, build uris, download files, and use Pandas to
do some simple cumulative actions.
`cd Exercises/Exercise-2` and see `README` in that location for instructions.

![image](https://github.com/user-attachments/assets/499bb349-2512-4d0a-a3f3-2ad71c07359f)
![image](https://github.com/user-attachments/assets/777c3393-361b-4261-afae-a103456c4ea1)
![image](https://github.com/user-attachments/assets/269ef954-b69c-4a4f-b30c-53a76b68cdab)

Sau khi build xong, truy cập file `main.py` bằng VS code

![image](https://github.com/user-attachments/assets/40b70aa4-c743-4ca2-8c2e-2d23a26b7ddd)
![image](https://github.com/user-attachments/assets/b7492cb5-c24e-4294-bfd0-33d5b7790250)



#### Exercise 3 - Boto3 AWS + s3 + Python.
The [third exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-3) tests a few skills.
This time we  will be using a popular `aws` package called `boto3` to try to perform a multi-step
actions to download some open source `s3` data files.
`cd Exercises/Exercise-3` and see `README` in that location for instructions.

Thay đổi đường dẫn thư mục tại CMD thành `Exercise-3`
Chạy lệnh docker `build --tag=exercise-3 .` để build image Docker (Quá trình diễn ra trong 2 – 3 phút)

![image](https://github.com/user-attachments/assets/9aefcfa8-1cba-4c86-b4da-44cda3875ff3)
![image](https://github.com/user-attachments/assets/d386cbc4-7caf-423c-a210-6c306802cb9c)

 Sau khi build xong, truy cập file `main.py` bằng VS code
![image](https://github.com/user-attachments/assets/8fcf455e-caa1-469d-9df6-0ba6342ab375)



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
![image](https://github.com/user-attachments/assets/0928de5f-23ae-4627-8d5c-ebe61d561742)
![image](https://github.com/user-attachments/assets/7eacd041-6c36-46f4-87c6-35d4d95ad25a)
![image](https://github.com/user-attachments/assets/b9898fba-76bb-4c98-bb41-41063d7874be)
![image](https://github.com/user-attachments/assets/8a4df515-84cf-4421-8a36-b127417d8ecc)
![image](https://github.com/user-attachments/assets/677c51d6-e86d-4e48-957c-2ad2e9ec4203)
![image](https://github.com/user-attachments/assets/c4e9ffaf-7ffd-44f5-9f75-6eeac5019547)
![image](https://github.com/user-attachments/assets/8bf42004-3a92-4625-bfa9-70eae81e8b96)


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
