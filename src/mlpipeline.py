import logging as log
from app.thread.kafkaProducerThread import KafkaProducerThread
from app.thread.kafkaConsumerThread import KafkaConsumerThread
from app.thread.mlModelTrainerThread import MLModelTrainerThread


if __name__ == "__main__":
    log.warning('Application start')
    kafka_prod = KafkaProducerThread()
    kafka_consume = KafkaConsumerThread()
    ml_model_trainer = MLModelTrainerThread()

    while True:
        if not kafka_prod.is_thread_alive:
            kafka_prod.is_thread_alive = True
            kafka_prod.start()

        if not kafka_consume.is_thread_alive:
            kafka_consume.is_thread_alive = True
            kafka_consume.start()

        if not ml_model_trainer.is_thread_alive:
            ml_model_trainer.is_thread_alive = True
            ml_model_trainer.start()
