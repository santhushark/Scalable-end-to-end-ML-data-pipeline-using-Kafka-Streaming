import logging as log
import app.config.GlobalConstants as globals
import time
from threading import Thread
from kafka import KafkaProducer
from json import dumps


class MLModelTrainerThread(Thread):
    __instance = None

    def __init__(self, thread_status=False):
        """ Constructor"""
        Thread.__init__(self)
        self.is_thread_alive = thread_status

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

            time.sleep(10)
            self.is_thread_alive = True
