import logging as log
import app.config.GlobalConstants as globals
import time
from threading import Thread
from kafka import KafkaProducer
from json import dumps
from app.thread.mlModelTrainerThread import MLModelTrainerThread


class KafkaProducerThread(Thread):
    __instance = None

    def __init__(self, thread_status=False):
        """ Constructor"""
        Thread.__init__(self)
        self.is_thread_alive = thread_status
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

        if KafkaProducerThread.__instance is None:
            KafkaProducerThread.__instance = self
        else:
            log.error("cannot create another instance: Kafka Producer")
            raise Exception("You cannot create another Kafka Producer Instance")

    @staticmethod
    def get_instance():
        """ Static access method. """
        if KafkaProducerThread.__instance is None:
            KafkaProducerThread()
        return KafkaProducerThread.__instance

    def run(self):
        ml_model_trainer = MLModelTrainerThread.get_instance()
        print("START: Kafka Test Data Producer Thread")
        while self.is_thread_alive:
            self.is_thread_alive = False
            if ml_model_trainer.test_df is not None:
                self.input_test_data_to_kafka_stream(ml_model_trainer.test_df)

            time.sleep(10)
            self.is_thread_alive = True

    def input_test_data_to_kafka_stream(self, test_df):
        for i in range(test_df.shape[0]):
            sample_df = self.get_random_sample(test_df)
            data = {"sample": sample_df.to_dict()}
            self.producer.send('sample', value=data)
            print("MESSAGE SENT")
            time.sleep(15)

    def get_random_sample(self, test_df):
        test = test_df.sample(n=1)
        return test

