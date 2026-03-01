from flask import Flask
from flask_cors import CORS

app = Flask(__name__)

CORS(app, origins=["http://localhost:5173"])

@app.route('/')
def hello():
    return 'Hello from Petwell!'

@app.route("/get-signed-url", methods=["POST"])
def get_signed_url():
    # ... your existing logic ...

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
