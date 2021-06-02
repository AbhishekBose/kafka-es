from confluent_kafka import Producer
import uuid
import json
import time
import random



class LogParser:

    @staticmethod
    def fetch_log(file_obj):
        for row in file_obj:
            yield row

    @staticmethod
    def read_log_file():
        filename = "../newrelic_agent.log"
        return open(filename, "r")

    @staticmethod
    def serialize_log(log):
        log.strip()
        get_message = log.split(" ")
        if len(get_message):
            message = " ".join(get_message[7:])
            message_type = get_message[5]
            if message_type not in ["INFO", "ERROR", "CRITICAL", "WARNING"]:
                return None
            date = get_message[0][1:]
            timestamp = get_message[1]
            datetime = date + " " + timestamp
        log_dict = {
            "message": message.strip(),
            "timestamp": datetime,
            "type": message_type
        }
        return log_dict



class KafkaProducer:

    def __init__(self):
        self.bootstrap_servers = "localhost:29092"
        self.topic = "data_log"
        self.p = Producer({'bootstrap.servers': self.bootstrap_servers})

    def produce(self, msg):

        serialized_message = json.dumps(msg)
        self.p.produce(serialized_message)
        self.p.poll(0)
        time.sleep(1)
        # self.p.flush()


if __name__ == '__main__':
    logFile = LogParser.read_log_file()
    logFileGen = LogParser.fetch_log(logFile)
    producer = KafkaProducer()
    while True:
        try:
            data = next(logFileGen)
            serialized_data = LogParser.serialize_log(data)
            print("Message is :: {}".format(serialized_data))
            producer.produce(serialized_data)
        except StopIteration:
            exit()
        except KeyboardInterrupt:
            print("Printing last message before exiting :: {}".format(serialized_data))
            exit()

