import logging as log
import app.config.GlobalConstants as globals
import time
from threading import Thread
from kafka import KafkaProducer
from json import dumps


class KafkaProducerThread(Thread):
    __instance = None

    def __init__(self, thread_status=False):
        """ Constructor.
        """
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
        while self.is_thread_alive:
            self.is_thread_alive = False
            data = {'MYID': 'A20501893'}
            self.producer.send("sample", value=data)
            time.sleep(10)
            self.is_thread_alive = True
