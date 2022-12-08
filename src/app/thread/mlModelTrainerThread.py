import logging as log
from threading import Thread
from pyspark.sql import *
import subprocess
import time
import pickle
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import pandas as pd
import shutil
import os

class MLModelTrainerThread(Thread):
    __instance = None

    def __init__(self, thread_status=False):
        """ Constructor"""
        Thread.__init__(self)
        self.is_thread_alive = thread_status
        self.spark_session = None
        self.test_df = None
        self.is_model_trained = False
        self.new_model_available = False

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
        print("START: Model Trainer Thread")

        #Uncomment when running on aws emr
        #self.create_spark_session()

        while self.is_thread_alive:
            self.is_thread_alive = False
            # print("111111111111111111111111111")

            #Uncomment when running on aws emr
            #if self.is_file_exists():

            if self.is_file_exists_local():
                spark = self.spark_session

                #Uncomment when running on aws emr
                # df = spark.read.csv("hdfs://ip-172-31-0-181.ec2.internal:8020/dataset/boston.csv", inferSchema=True,
                #                 header=True)

                df = pd.read_csv("/home/santosh/Desktop/MASTERS/STUDIES_MSDS/SEM_3_fall/Big_Data_Technologies/Project"
                                 "/git_work_big_data/Scalable-end-to-end-ML-data-pipeline/Dataset/boston.csv")

                #uncomment when running on aws emr
                # df = df.toPandas()

                X = df.iloc[:, :13]
                y = df.iloc[:, 13:14]
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25)

                self.test_df = X_test
                model = LinearRegression()
                model.fit(X_train, y_train)

                #uncomment this line when running on aws emr
                #filename = '/home/hadoop/Scalable-end-to-end-ML-data-pipeline/MLmodel/model.sav'

                filename = '/home/santosh/Desktop/MASTERS/STUDIES_MSDS/SEM_3_fall/Big_Data_Technologies/Project/' \
                           'git_work_big_data/Scalable-end-to-end-ML-data-pipeline/MLmodel/model.sav'
                pickle.dump(model, open(filename, 'wb'))

                #uncomment when running on aws emr
                #if self.is_mv_dataset_within_hdfs_success("/dataset/boston.csv", "/dataset/used/"+"boston"+str(int(time.time()))+".csv"):

                if self.is_mv_dataset_within_local_machine_success("/home/santosh/Desktop/MASTERS/STUDIES_MSDS/SEM_3_fall/Big_Data_Technologies/Project"
                                 "/git_work_big_data/Scalable-end-to-end-ML-data-pipeline/Dataset/boston.csv",
                                                          "/home/santosh/Desktop/MASTERS/STUDIES_MSDS/SEM_3_fall/Big_Data_Technologies/Project"
                                 "/git_work_big_data/Scalable-end-to-end-ML-data-pipeline/Dataset/used/" + "boston" + str(int(time.time())) + ".csv"):
                    print("Model Training: FAILURE")
                else:
                    self.is_model_trained = True
                    self.new_model_available = True
                    print("Model Training: SUCCESS")
            time.sleep(10)
            self.is_thread_alive = True

    # def test_train_split(df):
    #     vector_assembler = VectorAssembler(
    #         inputCols=['crim', 'zn', 'indus', 'chas', 'nox', 'rm', 'age', 'dis', 'rad', 'tax', 'ptratio', 'black',
    #                    'lstat'], outputCol='features')
    #     df_tf = vector_assembler.transform(df)
    #     df_tf = df_tf.select(['features', 'medv'])
    #
    #     return df_tf.randomSplit([0.7, 0.3])

    def create_spark_session(self):
        self.spark_session = SparkSession.builder.getOrCreate()

    def is_file_exists(self):
        proc = subprocess.Popen(['hadoop', 'fs', '-test', '-e', '/dataset/boston.csv'])
        proc.communicate()
        if proc.returncode == 0:
            return True
        else:
            return False

    def is_file_exists_local(self):
        return os.path.exists("/home/santosh/Desktop/MASTERS/STUDIES_MSDS/SEM_3_fall/Big_Data_Technologies/Project"
                                 "/git_work_big_data/Scalable-end-to-end-ML-data-pipeline/Dataset/boston.csv")

    def is_mv_dataset_within_hdfs_success(self, src_path, dst_path):
        proc = subprocess.Popen(["hdfs", "fs", "-mv", "-f", src_path, dst_path])
        proc.communicate()
        if proc.returncode == 0:
            return True
        else:
            return False

    def is_mv_dataset_within_local_machine_success(self, src_path, dst_path):
        shutil.move(src_path, dst_path)

        return False
