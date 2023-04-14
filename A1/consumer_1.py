import pulsar
import time
# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650') 

# Subscribe to a topic and subscription
consumer = client.subscribe('DEtopic_v1', subscription_name='DE-sub') 

# Define empty string
resultant_string = ""

# Declare msg
msg = {}

# Receive Total length
total = consumer.receive()
print("Received message : '%s'" % total.data())

# Convert the decoded message data to an integer
total_length = int(total.data().decode())

# Display message received from producer
for i in range(total_length):
    msg = consumer.receive()
    resultant_string += msg.data().decode() + ' '
    try:
        #print("Received message : %s" , msg.data().decode())

        # Acknowledge for receiving the message
        consumer.acknowledge(msg)
        print("Resultant String Test: {}".format(resultant_string))

    except: 
        consumer.negative_acknowledge(msg[i])

# Print Uppercase sentence
print("Resultant String: {}".format(resultant_string))

# Destroy pulsar client
client.close()
