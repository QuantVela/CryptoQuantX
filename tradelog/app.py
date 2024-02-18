import os
from flask import Flask, jsonify, request, abort
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import pandas as pd
import requests
import json

app = Flask(__name__)
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.file']
credential = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(credential)
gsheet = client.open("freqtrade tradelog").sheet1

def query_price():
    spot_base_url = "https://api.binance.com"
    spot_response = requests.get(f"{spot_base_url}/api/v3/ticker/price")
    spot_data = spot_response.json()  
    return spot_data

def update_prices():
    gsheet = client.open("freqtrade tradelog").worksheet('BreakoutCatcher')
    spot_data = query_price()
    spot_data_dict = {item['symbol']: item['price'] for item in spot_data}
    records = gsheet.get_all_records()

    # 遍历每行数据
    for i, record in enumerate(records, start=2): 
        symbol = record.get('交易币对', '').replace('/', '') # 例如：将"SUI/USDT"转换为"SUIUSDT"
        sell_time = record.get('卖出时间') 
        
        # 根据条件更新价格或清空价格
        if symbol in spot_data_dict and not sell_time:  # 如果找到价格且卖出时间为空
            price = spot_data_dict[symbol]
            gsheet.update_cell(i, 16, price)  # 更新对应行的第16列为当前价格
        elif sell_time:  # 如果卖出时间不为空
            gsheet.update_cell(i, 16, '')  # 清空对应行的第16列

def find_previous_total(gsheet, row_number, initial_capital):
    gsheet_data = gsheet.col_values(13)
    total_series = pd.Series(gsheet_data).replace('', pd.NA).astype(float).fillna(method='ffill')

    if pd.isna(total_series.iloc[0]):
        total_series.iloc[0] = initial_capital

    # 返回指定行号之前的最后一个有效值
    return total_series.iloc[row_number - 1]

    # while row_number > 1:  # 从当前行号开始向上遍历，直到第一行
    #     row_number -= 1  # 移动到上一行
    #     total_value = gsheet.cell(row_number, 13).value  # 尝试获取这一行的总资金值
    #     if total_value:  # 如果这一行有总资金值
    #         try:
    #             return float(total_value)  # 返回这个值转换成浮点数
    #         except ValueError:
    #             # 如果转换失败，继续向上遍历
    #             continue
    # return initial_capital  # 如果所有上面的行都没有值，返回初始资本

@app.route('/read_sheet', methods=["GET"])
def read_sheet():
    data = gsheet.get_all_records()
    return jsonify(data)

@app.route('/add_tradelog', methods=['POST'])
def add_tradelog():
    
    data = request.get_json()
    initial_capital = 5000  # 初始资金设定为 5000

    strategy = data.get('strategy', 'Unknown Strategy')
    if strategy == "Break1h":
        gsheet = client.open("freqtrade tradelog").worksheet('BreakoutCatcher')

    if 'enter_tag' in data:
        trade_id = data.get('trade_id', 'N/A')
        pair = data.get('pair', 'N/A')
        direction = data.get('direction', 'N/A')
        leverage = data.get('leverage', 'N/A')
        open_rate = float(data.get('open_rate', 0))
        amount = float(data.get('amount', 0))
        fees = open_rate * amount * 0.00075
        open_date_str = data.get('open_date', 'N/A')
        open_date_obj = datetime.strptime(open_date_str, '%Y-%m-%d %H:%M:%S.%f%z')
        open_date = open_date_obj.strftime('%Y-%m-%d %H:%M:%S')
        stake_amount = '{:.2f}'.format(float(data['stake_amount']))
        stake_currency = data.get('stake_currency', 'N/A')
        base_currency = data.get('base_currency', 'N/A')
        fiat_currency = data.get('fiat_currency', 'N/A')
        order_type = data.get('order_type', 'N/A')
        current_rate = data.get('current_rate', 'N/A')
        enter_tag = data.get('enter_tag', 'N/A')

        row_data = [trade_id, pair, open_date, open_rate, amount, stake_amount] + [''] * 4 + [fees]
        gsheet.append_row(row_data)
        return jsonify({"message": "Trade logged successfully"})
    
    if 'exit_reason' in data:
        trade_id = data.get('trade_id', 'N/A')
        pair = data.get('pair', 'N/A')
        direction = data.get('direction', 'N/A')
        leverage = data.get('leverage', 'N/A')
        gain = data.get('gain', 'N/A')
        close_rate = float(data.get('close_rate', 0))
        amount = float(data.get('amount', 0))
        open_rate = data.get('open_rate', 'N/A')
        current_rate = data.get('current_rate', 'N/A')
        profit_amount = float(data['profit_amount'])
        profit_ratio = '{:.2%}'.format(float(data['profit_ratio']))
        stake_currency = data.get('stake_currency', 'N/A')
        base_currency = data.get('base_currency', 'N/A')
        fiat_currency = data.get('fiat_currency', 'N/A')
        exit_reason = data.get('exit_reason', 'N/A')
        order_type = data.get('order_type', 'N/A')
        open_date = data.get('open_date', 'N/A')
        close_date_str = data.get('close_date', 'N/A')
        close_date_obj = datetime.strptime(close_date_str, '%Y-%m-%d %H:%M:%S.%f%z')
        close_date = close_date_obj.strftime('%Y-%m-%d %H:%M:%S')

        cell = gsheet.find(trade_id)
        if cell:
            row_number = cell.row
            # gsheet.update('G{}'.format(row_number), close_rate)  # 假设卖出时间对应第7列
            gsheet.update_cell(row_number, 7, close_date)
            gsheet.update_cell(row_number, 8, close_rate)  
            gsheet.update_cell(row_number, 9, amount)       
            gsheet.update_cell(row_number, 10, exit_reason)        
            gsheet.update_cell(row_number, 15, profit_ratio)   
            # fee
            existing_fees = float(gsheet.cell(row_number, 11).value) 
            new_fees = existing_fees + close_rate * amount * 0.00075
            gsheet.update_cell(row_number, 11, new_fees)
            # 总资金
            previous_total = find_previous_total(gsheet, row_number, initial_capital)
            new_total = previous_total + profit_amount
            gsheet.update_cell(row_number, 12, '{:.2f}'.format(profit_amount)) 
            gsheet.update_cell(row_number, 13, '{:.2f}'.format(new_total))
            # 更新PnL Ratio
            pnl_ratio = (profit_amount / new_total) * 100
            gsheet.update_cell(row_number, 14, '{:.2f}%'.format(pnl_ratio))
            # 更新WIN/LOSS
            win_loss = "WIN" if profit_amount > 0 else "LOSS"
            gsheet.update_cell(row_number, 18, win_loss)

            return jsonify({"message": "Trade logged successfully"})
        else:
            return jsonify({"error": "Trade ID not found"}), 404

# 设置APScheduler后台任务调度器
scheduler = BackgroundScheduler()
scheduler.add_job(func=update_prices, trigger="interval", minutes=5)
scheduler.start()

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=5000, use_reloader=False)