import logging as log
from threading import Thread

# USE below when running on aws emr
from pyspark.sql import *

import subprocess
import time
import pickle
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import pandas as pd
import shutil
import os
import app.config.GlobalConstants as globals


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

        # USE below when running on aws emr
        self.create_spark_session()

        while self.is_thread_alive:
            self.is_thread_alive = False
            # USE below when running on aws emr
            if self.is_file_exists():

            # USE below when running on local machine
            #if self.is_file_exists_local():
                # USE below when running on aws emr
                spark = self.spark_session

                # USE below when running on aws emr
                df = spark.read.csv(globals.HDFS_DATASET_LOC, inferSchema=True, header=True)

                # USE below when running on local machine
                #df = pd.read_csv(globals.LOCAL_MACHINE_DATASET_SRC)

                # USE below when running on aws emr
                df = df.toPandas()

                X = df.iloc[:, :13]
                y = df.iloc[:, 13:14]
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25)

                self.test_df = X_test
                model = LinearRegression()
                model.fit(X_train, y_train)

                # USE below when running on aws emr
                filename = globals.AWS_EMR_ML_MODEL_SAVE_LOC

                # USE below when running on local machine
                #filename = globals.ML_MODEL_SAVE_LOC

                pickle.dump(model, open(filename, 'wb'))


                # USE below when running on aws emr
                if self.is_mv_dataset_within_hdfs_success(globals.HDFS_DATASET_LOC_FOR_CMD, globals.HDFS_DATASET_USED_LOC_FOR_CMD +str(int(time.time()))+".csv"):
                # USE below when running on local machine
                #if self.is_mv_dataset_within_local_machine_success(globals.LOCAL_MACHINE_DATASET_SRC, globals.LOCAL_MACHINE_DATASET_USED_DST + str(int(time.time())) + ".csv"):
                    print("Model Training: FAILURE")
                else:
                    self.is_model_trained = True
                    self.new_model_available = True
                    print("Model Training: SUCCESS")
            time.sleep(10)
            self.is_thread_alive = True

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
        return os.path.exists(globals.LOCAL_MACHINE_DATASET_SRC)

    def is_mv_dataset_within_hdfs_success(self, src_path, dst_path):
        proc = subprocess.Popen(["hadoop", "fs", "-mv", src_path, dst_path])
        proc.communicate()
        if proc.returncode == 0:
            return True
        else:
            return False

    def is_mv_dataset_within_local_machine_success(self, src_path, dst_path):
        shutil.move(src_path, dst_path)

        return False
