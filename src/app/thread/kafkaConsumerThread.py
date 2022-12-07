import logging as log
import app.config.GlobalConstants as globals
import time
from threading import Thread
from json import loads
from kafka import KafkaConsumer
from pyspark.ml.regression import LinearRegressionModel

from src.app.thread.mlModelTrainerThread import MLModelTrainerThread


class KafkaConsumerThread(Thread):
    __instance = None

    def __init__(self, thread_status=False):
        """ Constructor"""
        Thread.__init__(self)
        self.is_thread_alive = thread_status
        self.consumer = KafkaConsumer('sample', bootstrap_servers=['localhost:9092'],
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True,
                                      group_id='my-group',
                                      value_deserializer=lambda x: loads(x.decode('utf-8')))
        self.lr_model = None
        if KafkaConsumerThread.__instance is None:
            KafkaConsumerThread.__instance = self
        else:
            log.error("cannot create another instance: Kafka Consumer")
            raise Exception("You cannot create another Kafka Consumer Instance")

    @staticmethod
    def get_instance():
        """ Static access method. """
        if KafkaConsumerThread.__instance is None:
            KafkaConsumerThread()
        return KafkaConsumerThread.__instance

    def run(self):
        ml_model_trainer = MLModelTrainerThread()
        while self.is_thread_alive:
            self.is_thread_alive = False
            if ml_model_trainer.is_model_trained:
                self.load_ml_model()
                self.consume_msgs_and_predict(ml_model_trainer)
            # for message in self.consumer:
            #     message = message.value
            #     for key, value in message.items():
            #         print("Key=", key, ", ", "Value=", value)
            print("THREAD - 3")
            time.sleep(5)
            self.is_thread_alive = True

    def load_ml_model(self):
        self.lr_model = LinearRegressionModel.load("/home/hadoop/Scalable-end-to-end-ML-data-pipeline/MLmodel/model")


    def consume_msgs_and_predict(self, ml_model_trainer):
        for message in self.consumer
            if ml_model_trainer.new_model_available:
                ml_model_trainer.new_model_available= False
                self.load_ml_model()
    #     message = message.value
    #     for key, value in message.items():
    #         print("Key=", key, ", ", "Value=", value)