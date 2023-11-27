from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_date
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_replace

S3_DATA_SOURCE_PATH = 'gs://us-west1-airflow-project-19f5b649-bucket/data_files/data.csv'  # CSV data file location
S3_DATA_OUTPUT_PATH = 'gs://us-west1-airflow-project-19f5b649-bucket/output_files'  # Output files saving location

def func_run():
    spark = SparkSession.builder.appName('KaggleDataETL').getOrCreate()
    all_data = spark.read.csv(S3_DATA_SOURCE_PATH, header=True)

    xiami = all_data.filter(all_data['brand'] == 'aardwolf').na.drop()
    xiami = xiami.select('event_time', 'event_type', 'category_code', 'product_id', 'price', 'user_id').dropDuplicates()

    def first_half(text):
        return text.split('.')[0] if text else None

    def str_dot_index(text):
        return text.split('.')[1] if text and '.' in text else None

    first_half_udf = udf(first_half, StringType())
    str_dot_index_udf = udf(str_dot_index, StringType())

    xiami = xiami.withColumn('category', first_half_udf(col('category_code'))) \
                 .withColumn('subcategory', str_dot_index_udf(col('category_code'))) \
                .withColumn('event_time', regexp_replace('event_time', ' UTC', '')) \
                .withColumn('event_date', to_date(col('event_time'), 'yyyy-MM-dd HH:mm:ss'))\
                .drop('category_code', 'event_time')

    data = xiami.drop('price').dropDuplicates()
    product_price = xiami.select('product_id', 'price').dropDuplicates()

    data.write.mode('overwrite').parquet(f'{S3_DATA_OUTPUT_PATH}/data')
    product_price.write.mode('overwrite').parquet(f'{S3_DATA_OUTPUT_PATH}/product_price')

    print('Total number of records: %s' % all_data.count())

if __name__ == "__main__":
    func_run()
