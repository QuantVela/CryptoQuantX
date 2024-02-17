import os
from flask import Flask, jsonify, request, abort
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.file']
credential = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(credential)
gsheet = client.open("freqtrade tradelog").sheet1

@app.route('/read_sheet', methods=["GET"])
def read_sheet():
    data = gsheet.get_all_records()
    return jsonify(data)

@app.route('/add_tradelog', methods=['POST'])
def add_tradelog():
    
    data = request.get_json()

    if 'enter_tag' in data:
        trade_id = data.get('trade_id', 'N/A')
        pair = data.get('pair', 'N/A')
        direction = data.get('direction', 'N/A')
        leverage = data.get('leverage', 'N/A')
        open_rate = data.get('open_rate', 'N/A')
        amount = data.get('amount', 'N/A')
        open_date = data.get('open_date', 'N/A')
        stake_amount = data.get('stake_amount', 'N/A')
        stake_currency = data.get('stake_currency', 'N/A')
        base_currency = data.get('base_currency', 'N/A')
        fiat_currency = data.get('fiat_currency', 'N/A')
        order_type = data.get('order_type', 'N/A')
        current_rate = data.get('current_rate', 'N/A')
        enter_tag = data.get('enter_tag', 'N/A')

        app.logger.debug(f"""Logging trade: Trade ID: {trade_id}, Pair: {pair}, Direction: {direction}, 
                            Leverage: {leverage}, Open Rate: {open_rate}, Amount: {amount}, 
                            Open Date: {open_date}, Stake Amount: {stake_amount}, Stake Currency: {stake_currency}, 
                            Base Currency: {base_currency}, Fiat Currency: {fiat_currency}, Order Type: {order_type}, 
                            Current Rate: {current_rate}, Enter Tag: {enter_tag}""")

        gsheet.append_row([trade_id, pair, open_date])
        return jsonify({"message": "Trade logged successfully"})
    
    if 'exit_reason' in data:
        trade_id = data.get('trade_id', 'N/A')
        pair = data.get('pair', 'N/A')
        direction = data.get('direction', 'N/A')
        leverage = data.get('leverage', 'N/A')
        gain = data.get('gain', 'N/A')
        close_rate = data.get('close_rate', 'N/A')           
        amount = data.get('amount', 'N/A')
        open_rate = data.get('open_rate', 'N/A')
        current_rate = data.get('current_rate', 'N/A')
        profit_amount = data.get('profit_amount', 'N/A')
        profit_ratio = data.get('profit_ratio', 'N/A')
        stake_currency = data.get('stake_currency', 'N/A')
        base_currency = data.get('base_currency', 'N/A')
        fiat_currency = data.get('fiat_currency', 'N/A')
        exit_reason = data.get('exit_reason', 'N/A')
        order_type = data.get('order_type', 'N/A')
        open_date = data.get('open_date', 'N/A')
        close_date = data.get('close_date', 'N/A')

        app.logger.debug(f"""Logging trade: Trade ID: {trade_id}, Pair: {pair}, Direction: {direction}, 
                            Leverage: {leverage}, gain: {gain}, close_rate: {close_rate}, Amount: {amount}, 
                            open_rate: {open_rate},  Current Rate: {current_rate}, profit_amount: {profit_amount},
                            profit_ratio: {profit_ratio}, Stake Currency: {stake_currency}, Base Currency: {base_currency}, Fiat Currency: {fiat_currency},
                            exit_reason: {exit_reason}, Order Type: {order_type}, Open Date: {open_date},       
                            close_date: {close_date}""")
        gsheet.append_row([trade_id, pair, open_date])

        return jsonify({"message": "Trade logged successfully"})

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=False, port=5000)