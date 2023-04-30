from flask import Flask, json, request
import os
import uuid
from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential

# Cosmos DB NOSQL config
endpoint = os.environ["COSMOS_ENDPOINT"]
DATABASE_NAME = "cosmicworks"
CONTAINER_NAME = "data"
credential = DefaultAzureCredential()
client = CosmosClient.from_connection_string("AccountEndpoint=https://endpointcosmos.documents.azure.com:443/;AccountKey=m8LD7A5sWsiAqvfPHkGBkQCepDNebI2iunln8GA8fypznI8aFkgNHCTrr1dSwpoldmVswl3oWO1VACDbv1s84A==;")

database = client.get_database_client(DATABASE_NAME)

container = database.get_container_client(CONTAINER_NAME)

app = Flask(__name__)


@app.route("/", methods=['GET'])
def hello():
    return "Hello, World", 200


@app.route("/collect", methods=['POST'])
def process_json():
    data = json.loads(request.data)
    data["id"] = str(uuid.uuid4())
    container.create_item(data)
    return data, 201


if __name__ == "__main__":
    app.run()
