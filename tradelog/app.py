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
    if not request.is_json:
        abort(400, 'Request must be JSON')
    
    data = request.get_json()

    if 'entry_fill' in data:
        for trade in data['entry_fill']:
            trade_id = trade.get('trade_id', 'N/A')
            pair = trade.get('pair', 'N/A')
            direction = trade.get('direction', 'N/A')
            leverage = trade.get('leverage', 'N/A')
            open_rate = trade.get('open_rate', 'N/A')
            amount = trade.get('amount', 'N/A')
            open_date = trade.get('open_date', 'N/A')
            stake_amount = trade.get('stake_amount', 'N/A')
            stake_currency = trade.get('stake_currency', 'N/A')
            base_currency = trade.get('base_currency', 'N/A')
            fiat_currency = trade.get('fiat_currency', 'N/A')
            order_type = trade.get('order_type', 'N/A')
            current_rate = trade.get('current_rate', 'N/A')
            enter_tag = trade.get('enter_tag', 'N/A')

            app.logger.debug(f"Logging trade: Trade ID: {trade_id}, Pair: {pair}, Direction: {direction}, 
                             Leverage: {leverage}, Open Rate: {open_rate}, Amount: {amount}, 
                             Open Date: {open_date}, Stake Amount: {stake_amount}, Stake Currency: {stake_currency}, 
                             Base Currency: {base_currency}, Fiat Currency: {fiat_currency}, Order Type: {order_type}, 
                             Current Rate: {current_rate}, Enter Tag: {enter_tag}")

            gsheet.append_row([trade_id, pair, open_date])
        return jsonify({"message": "Trade logged successfully"})
    
    if 'exit_fill' in data:
        for trade in data['exit_fill']:
            trade_id = trade.get('trade_id', 'N/A')
            pair = trade.get('pair', 'N/A')
            direction = trade.get('direction', 'N/A')
            leverage = trade.get('leverage', 'N/A')
            gain = trade.get('gain', 'N/A')
            close_rate = trade.get('close_rate', 'N/A')           
            amount = trade.get('amount', 'N/A')
            open_rate = trade.get('open_rate', 'N/A')
            current_rate = trade.get('current_rate', 'N/A')
            profit_amount = trade.get('profit_amount', 'N/A')
            profit_ratio = trade.get('profit_ratio', 'N/A')
            stake_currency = trade.get('stake_currency', 'N/A')
            base_currency = trade.get('base_currency', 'N/A')
            fiat_currency = trade.get('fiat_currency', 'N/A')
            exit_reason = trade.get('exit_reason', 'N/A')
            order_type = trade.get('order_type', 'N/A')
            open_date = trade.get('open_date', 'N/A')
            close_date = trade.get('close_date', 'N/A')

            gsheet.append_row([trade_id, pair, open_date])
        return jsonify({"message": "Trade logged successfully"})

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=5000)