from flask import Flask, json, request

app = Flask(__name__)


@app.route("/", methods=['GET'])
def hello():
    return "Hello, World", 200


@app.route("/collect", methods=['POST'])
def process_json():
    data = json.loads(request.data)
    return data, 201


if __name__ == "__main__":
    app.run()
