import requests
from lxml import etree
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StringType

# Function to extract bios from a given URL
def extract_bio(bio_url):
    response = requests.get(bio_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        bio_element = soup.find("div", class_="profile__bio-copy")
        if bio_element:
            bio = bio_element.get_text(strip=True)
            return bio
    return None

# Fetch bio URLs
page = requests.get('https://www.law.uchicago.edu/directory?profile_type=103')
html_content = page.content
html_tree = etree.HTML(html_content)
xpath_expression = '//a/@href'
links = html_tree.xpath(xpath_expression)

bio_urls = ['https://www.law.uchicago.edu' + link for link in links if '/faculty/' in link]
bio_urls = list(set(bio_urls))

# Extract bios using Spark
spark = SparkSession.builder \
    .appName("BioExtraction") \
    .getOrCreate()

# Create a DataFrame with bio URLs
bio_urls_df = spark.createDataFrame(bio_urls, StringType()).toDF("bio_url")

# Register the extract_bio function as a UDF (User Defined Function)
spark.udf.register("extract_bio_udf", extract_bio, StringType())

# Apply the UDF to extract bios
bios_df = bio_urls_df.select(col("bio_url"), expr("extract_bio_udf(bio_url) as bio"))

# Collect the bios and print them
bios = bios_df.collect()
for bio_row in bios:
    print(bio_row.bio)

# Write bios to a text file
bios_rdd = bios_df.rdd.map(lambda x: x.bio)
bios_rdd.saveAsTextFile("profbio.txt")

# Stop the Spark session
spark.stop()
