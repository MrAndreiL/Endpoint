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
client = CosmosClient(url=endpoint, credential=credential)

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
