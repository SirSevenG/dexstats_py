import mysql.connector
import json


class MYSQLconn:
    def __init__(self, creds: dict):
        self.cnx = mysql.connector.connect(**creds)
        self.cur = self.cnx.cursor(dictionary=True)

    def get_market(self, ret_json=False):
        query = "SELECT DISTINCT taker_coin,maker_coin FROM swaps WHERE started_at >= now() - INTERVAL 1 DAY"
        self.cur.execute(query)
        res = self.cur.fetchall()
        if ret_json:
            return json.dumps(res)
        return res

    def get_trades(self, pairs: list) -> list:
        req = """
                 (SELECT uuid,started_at,maker_amount AS makerVolume,taker_amount
                 AS volume,(maker_amount / taker_amount)
                 AS price FROM swaps WHERE taker_coin = "{}" AND maker_coin = "{}"
                 AND started_at >= now() - INTERVAL 1 DAY) UNION (SELECT uuid,started_at,taker_amount
                 AS takerVolume,maker_amount
                 AS volume,(taker_amount / maker_amount)
                 AS price FROM swaps WHERE taker_coin = "{}"
                 AND maker_coin = "{}" AND started_at >= now() - INTERVAL 1 DAY) ORDER BY started_at
              """
        tickers = []
        for p in pairs:
            pair = [p.get('taker_coin'), p.get('maker_coin')]
            query = req.format(pair[0], pair[1], pair[1], pair[0])
            self.cur.execute(query)
            sqlres = self.cur.fetchall()
            res = {
                'market': pair[0]+'-'+pair[1],
                'trades24h': len(sqlres)
            }
            tickers.append([res, {'trades': sqlres}])
        return json.dumps(tickers, indent=4, sort_keys=True, default=str)
