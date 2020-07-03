

from flask import Flask, request, abort
from log import log
from mqevent import *
from MongoDataServer import db
import json

app = Flask(__name__)
mq=Mqserver()

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'POST':
        data = request.get_data(as_text=True)
        try:
            data=json.loads(data)
            feed=data["exchange"]
            key=data["ticker"]
            e = Event(exchange="Alert", routing_key=f"{feed}.{key}", data=data)
            mq.publish(e)
            dbname=data['alert']
            db.insert_one(dbname,data)
            log.info(e)
        except Exception as err:
            log.debug(err)
        return "'"



if __name__ == '__main__':

    app.run(host="0.0.0.0", port=80,debug=True)