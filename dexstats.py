from flask import Flask
from dbconnection import MYSQLconn
app = Flask(__name__)
creds = {
        'user': 'swaps',
        'password': 'pass',
        'host': '127.0.0.1',
        'database': 'swaps',
        'port': 3306
    }
dbconn = MYSQLconn(creds)


@app.route('/ticker')
def ticker():
    res = dbconn.get_trades()
    return res


@app.route('/trades')
def trades():
    pairs = dbconn.get_market()
    res = dbconn.get_trades(pairs)
    return res
