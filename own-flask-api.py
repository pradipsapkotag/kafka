from flask import Flask,jsonify
import json

app = Flask(__name__)

@app.route('/products/<int:ide>', methods = ['GET'])
def get_products(ide):
    try:
        with open ('api-data-list.json','r') as f:
                data = json.loads(f.read())
        return data[ide] 
    except:
        status = {"status":"failed",
                  "message":"either invalid id or product unavailable"}
        return(jsonify(status))


app.run(debug=True)