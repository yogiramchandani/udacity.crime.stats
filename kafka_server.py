import producer_server
from pathlib import Path

# kafka-console-consumer --bootstrap-server localhost:9092 --topic udacity.project.2.crime --from-beginning
def run_kafka_server():
	# TODO get the json file path
    input_file = f"{Path(__file__).parents[0]}/police-department-calls-for-service.json"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="udacity.project.2.crime",
        bootstrap_servers="localhost:9092",
        client_id="kafka.python.producer.crime"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
