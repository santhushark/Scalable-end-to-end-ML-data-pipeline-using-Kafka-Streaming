import logging as log
from app.thread.kafkaProducerThread import KafkaProducerThread
from app.thread.kafkaConsumerThread import KafkaConsumerThread

if __name__ == "__main__":
    log.warning('Application start')
    tempSensor = TemperatureSensorThread()

    while True:
        if not tempSensor.is_thread_alive:
            tempSensor.is_thread_alive = True
            tempSensor.start()
