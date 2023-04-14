import pulsar
# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Create a producer on the topic that consumer can subscribe to
producer = client.create_producer('DEtopic_v1') 

# Input string
INPUT_STRING = "I want to be capatilized"

if __name__ == "__main__":
    split_string = INPUT_STRING.split(" ")
    total_length = len(split_string)
    # Send a message to consumer: Total length
    producer.send(str(total_length).encode('utf-8'))

    for txt in split_string:
        message = txt.upper()

        # Send a message to consumer
        producer.send((message).encode('utf-8'))

# Destroy pulsar client
client.close()
