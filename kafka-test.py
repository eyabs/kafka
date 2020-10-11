import threading
import time
import json
from random import seed, uniform
from kafka import KafkaConsumer, KafkaProducer

KAFKA_TOPIC = 'test'
MAX_MESSAGES_PRODUCE = 25
MAX_MESSAGES_CONSUME = 25

seed(47)

# Make a process which produces messages at a random interval
def message_producer(topic, max_messages):
    print('Producer starting')
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    except Exception as e:
        print(f"Error creating producer: {e}")
        return
    # loop up to MAX_MESSAGES times
    message_count = 0
    while message_count < max_messages:
        # publish a message
        message = f"Message #{message_count}"
        producer.send(topic, message)
        print(f"producing {message}")
        message_count += 1
        # sleep between 1 and 5 seconds
        if message_count < max_messages:
            time.sleep(uniform(1, 5))

    print('Done sending messages.')


# Make a process which consumes messages at a random interval
def message_consumer(topic, max_messages):
    print('Consumer starting')
    try:
        consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')
    except Exception as e:
        print(f"Error creating consumer: {e}")
        return

    # consume messages until MAX_MESSAGES has been reached
    message_count = 0
    while message_count < max_messages:
        msg = next(consumer)
        print(f"Consuming message: {msg}")
        message_count += 1

    print('Done consuming messages.')


def main():
    # Create the producer thread    
    producer_thread = threading.Thread(
        target=message_producer, args=(KAFKA_TOPIC, MAX_MESSAGES_PRODUCE))

    # Create the consumer thread
    consumer_thread = threading.Thread(
        target=message_consumer, args=(KAFKA_TOPIC, MAX_MESSAGES_CONSUME))


    # Start the threads
    producer_thread.start()
    consumer_thread.start()

    # join the threads
    producer_thread.join()
    consumer_thread.join()


if __name__ == '__main__':
    main()