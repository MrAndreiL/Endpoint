from flask import Flask, json, request
import uuid
import sys
import logging
from azure.cosmos import CosmosClient
from azure.messaging.webpubsubservice import WebPubSubServiceClient

# Cosmos DB NOSQL config
logger = logging.getLogger('azure')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)

DATABASE_NAME = "cosmicworks"
CONTAINER_NAME = "data"
client = CosmosClient.from_connection_string("AccountEndpoint=https://endpointcosmos.documents.azure.com:443/;AccountKey=m8LD7A5sWsiAqvfPHkGBkQCepDNebI2iunln8GA8fypznI8aFkgNHCTrr1dSwpoldmVswl3oWO1VACDbv1s84A==;")

database = client.get_database_client(DATABASE_NAME)

container = database.get_container_client(CONTAINER_NAME)

# pubsub service
service = WebPubSubServiceClient.from_connection_string("Endpoint=https://endpointpubsub.webpubsub.azure.com;AccessKey=tTTisi1heGR+td4qdXuGApTR1ZVH/P/cV12VeTvKin4=;Version=1.0;",
                                                        hub='hub')

app = Flask(__name__)


@app.route("/", methods=['GET'])
def hello():
    return "Hello, World", 200


@app.route("/collect", methods=['POST'])
def process_json():
    data = json.loads(request.data)
    data["id"] = str(uuid.uuid4())
    container.create_item(data)
    service.send_to_all(data, content_type="application/json")
    return data, 201


if __name__ == "__main__":
    app.run()
