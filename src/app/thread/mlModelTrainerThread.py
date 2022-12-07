import logging as log
import time
from threading import Thread
from pyspark.sql import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression


class MLModelTrainerThread(Thread):
    __instance = None

    def __init__(self, thread_status=False):
        """ Constructor"""
        Thread.__init__(self)
        self.is_thread_alive = thread_status
        self.spark_session = None

        if MLModelTrainerThread.__instance is None:
            MLModelTrainerThread.__instance = self
        else:
            log.error("cannot create another instance: ML Model Trainer")
            raise Exception("You cannot create another Machine Learning model trainer")

    @staticmethod
    def get_instance():
        """ Static access method. """
        if MLModelTrainerThread.__instance is None:
            MLModelTrainerThread()
        return MLModelTrainerThread.__instance

    def run(self):
        while self.is_thread_alive:
            self.is_thread_alive = False
            print("THREAD -2")
            self.create_spark_session()
            spark = self.spark_session
            df = spark.read.csv("hdfs://ip-172-31-0-181.ec2.internal:8020/dataset/boston.csv", inferSchema=True,
                                header=True)

            time.sleep(10)
            self.is_thread_alive = True

    def test_train_split(df):
        vectorAssembler = VectorAssembler(
            inputCols=['crim', 'zn', 'indus', 'chas', 'nox', 'rm', 'age', 'dis', 'rad', 'tax', 'ptratio', 'black',
                       'lstat'], outputCol='features')
        df_tf = vectorAssembler.transform(df)
        df_tf = df_tf.select(['features', 'medv'])

        return df_tf.randomSplit([0.7, 0.3])

    def create_spark_session(self):
        self.spark_session = SparkSession.builder.getOrCreate()
