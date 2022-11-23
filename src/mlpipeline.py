import logging as log
from app.thread.kafkaProducerThread import KafkaProducerThread
from app.thread.kafkaConsumerThread import KafkaConsumerThread

if __name__ == "__main__":
    log.warning('Application start')
    kafkaProd = KafkaProducerThread()
    kafkaConsume = KafkaConsumerThread()

    while True:
        if not kafkaProd.is_thread_alive:
            kafkaProd.is_thread_alive = True
            kafkaProd.start()

        if not kafkaConsume.is_thread_alive:
            kafkaConsume.is_thread_alive = True
            kafkaConsume.start()
