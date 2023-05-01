from flask import Flask
from azure.messaging.webpubsubservice import WebPubSubServiceClient
import asyncio
import json
import logging
import websockets
import sys
from azure.cosmos import CosmosClient
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage

DATABASE_NAME = "cosmicworks"
CONTAINER_NAME = "data"
client = CosmosClient.from_connection_string("AccountEndpoint=https://enrichedendpoint.documents.azure.com:443/;AccountKey=XfA920EiLEQ41DPDxDhfD0NVMc7Hl5MsYgvuQlPVbRD7Wxvs43wbMZTQoIai7UN8JVTSHhU3sNANACDbDMtdXQ==;")
database = client.get_database_client(DATABASE_NAME)
container = database.get_container_client(CONTAINER_NAME)

NAMESPACE_CONNECTIONS_STR = "Endpoint=sb://enrichedbusdata.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=rq2dOYPuecCByEa7ihmSwNPUEVIUFe5Kc+ASbDYQL6s="
QUEUE_NAME = "dataqueue"
logger = logging.getLogger('azure')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)

app = Flask(__name__)

bill = 0.75


async def connect(url):
    async with websockets.connect(url) as ws:
        logger.debug("connected")
        while True:
            message = await ws.recv()
            message = json.loads(message)
            message['bill'] = bill * message['watts']
            container.create_item(message)
            client = ServiceBusClient.from_connection_string(NAMESPACE_CONNECTIONS_STR)
            sender = client.get_queue_sender(QUEUE_NAME)
            single_message = ServiceBusMessage(str(message))
            await sender.send_messages(single_message)
            logger.debug("Received message")


@app.route("/", methods=['GET'])
def hello():
    return "Hello, World", 200


if __name__ == "__main__":
    connection_string = "Endpoint=https://endpointpubsub.webpubsub.azure.com;AccessKey=tTTisi1heGR+td4qdXuGApTR1ZVH/P/cV12VeTvKin4=;Version=1.0;"
    hub_name = "hub"

    service = WebPubSubServiceClient.from_connection_string(connection_string, hub=hub_name)
    token = service.get_client_access_token()

    try:
        asyncio.get_event_loop().run_until_complete(connect(token['url']))
    except KeyboardInterrupt:
        pass
    app.run()
