from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = (
        SparkSession
        .builder
        .appName("Word Count")
        .getOrCreate()
    )

    data = [
        "spark flink mapreduce",
        "mapreduce spark"
    ]


    rdd = spark.sparkContext.parallelize(data,2)

    def mapper(s: str) -> list[tuple[str, int]]:
        return [(x,1) for x in s.split(" ")]

    def reducer(x: int, y: int) -> int:
        return x + y


print(
    rdd
    .flatMap(mapper)
    .reduceByKey(reducer)
    .collect()
)