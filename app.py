from flask import Flask, json, request
import uuid
import asyncio
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
from azure.cosmos import CosmosClient
app = Flask(__name__)

DATABASE_NAME = "cosmicworks"
CONTAINER_NAME = "data"
client = CosmosClient.from_connection_string("AccountEndpoint=https://endpointcosmos.documents.azure.com:443/;AccountKey=m8LD7A5sWsiAqvfPHkGBkQCepDNebI2iunln8GA8fypznI8aFkgNHCTrr1dSwpoldmVswl3oWO1VACDbv1s84A==;")

database = client.get_database_client(DATABASE_NAME)

container = database.get_container_client(CONTAINER_NAME)

NAMESPACE_CONNECTION_STRING = "Endpoint=sb://endpointbus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=xccJjKsTNebVapH1MqfOVI5hX+fJh68WN+ASbBTTm/A="
QUEUE_NAME = "endpointbusqueue"
@app.route("/", methods=['GET'])
def hello():
    return "Hello, World", 200

@app.route("/collections", methods=['POST'])
def process_json():
    data = json.loads(request.data)
    data["id"] = str(uuid.uuid4())
    container.create_item(data)
    # Send to bus.
    with ServiceBusClient.from_connection_string(NAMESPACE_CONNECTION_STRING) as client:
        message = ServiceBusMessage(data)
        sender = client.get_queue_sender(QUEUE_NAME)
        sender.send(message)
    return data, 201

if __name__ == "__main__":
    app.run()
