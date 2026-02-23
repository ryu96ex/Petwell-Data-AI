from flask import Flask
from receive_upload import pubsub_bp

app = Flask(__name__)
app.register_blueprint(pubsub_bp)

@app.get("/")
def hello():
    return "Hello from Petwell!"
