import logging as log
from app.thread.kafkaProducerThread import KafkaProducerThread
from app.thread.kafkaConsumerThread import KafkaConsumerThread
from app.thread.mlModelTrainerThread import MLModelTrainerThread

if __name__ == "__main__":
    log.warning('Application start')
    kafkaProd = KafkaProducerThread()
    kafkaConsume = KafkaConsumerThread()
    mlModelTrainer = MLModelTrainerThread()

    while True:
        if not kafkaProd.is_thread_alive:
            kafkaProd.is_thread_alive = True
            kafkaProd.start()

        if not kafkaConsume.is_thread_alive:
            kafkaConsume.is_thread_alive = True
            kafkaConsume.start()

        if not mlModelTrainer.is_thread_alive:
            mlModelTrainer.is_thread_alive = True
            mlModelTrainer.start()

