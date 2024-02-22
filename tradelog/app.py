import os
from flask import Flask, jsonify, request, abort
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import requests
import json
import time

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

def sfloat(value, default=0.0):
    try:
        return float(value) if value else default
    except ValueError:
        return default
    
def update_prices():
    sheets = ['BreakoutCatcher', 'TrendCatcher']  # 定义要更新的工作表列表
    spot_data = query_price()
    spot_data_dict = {item['symbol']: item['price'] for item in spot_data}

    for sheet_name in sheets:
        gsheet = client.open("freqtrade tradelog").worksheet(sheet_name)
        time.sleep(30)
        expected_headers = ['ID', '交易币对', '买入时间', '买入价格', '买入数量', '加仓 1 买入价格', '加仓 1 买入数量', '加仓 2 买入价格', '加仓 2 买入数量' ,'U 数量', '卖出时间' ,'卖出价格', '卖出数量', '卖出原因', 'Fees',	'PnL', '总资金', 'PnL Ratio', '单笔收益率', '当前价格', '未平仓盈亏', '未平仓盈亏%', 'WIN/LOSS'] 
        records = gsheet.get_all_records(expected_headers=expected_headers)

        for i, record in enumerate(records, start=2): 
            symbol = record.get('交易币对', '').replace('/', '') # 例如：将"SUI/USDT"转换为"SUIUSDT"
            sell_time = record.get('卖出时间')
            buy_price = sfloat(record.get('买入价格', 0))
            buy_amount = sfloat(record.get('买入数量', 0))
            fees = sfloat(record.get('Fees', 0))
            add_buy_price_1 = sfloat(record.get('加仓 1 买入价格', 0))
            add_buy_amount_1 = sfloat(record.get('加仓 1 买入数量', 0))
            add_buy_price_2 = sfloat(record.get('加仓 2 买入价格', 0))
            add_buy_amount_2 = sfloat(record.get('加仓 2 买入数量', 0))

            if symbol in spot_data_dict and not sell_time:  # 如果找到价格且卖出时间为空
                price = sfloat(spot_data_dict[symbol])
                gsheet.update_cell(i, 20, price)  # 更新当前价格

                total_amount = buy_amount + add_buy_amount_1 + add_buy_amount_2
                total_investment = (buy_price * buy_amount) + (add_buy_price_1 * add_buy_amount_1) + (add_buy_price_2 * add_buy_amount_2)
                unrealized_pnl = price * total_amount - total_investment - fees
                unrealized_pnl_pct = unrealized_pnl / total_investment if total_investment else 0

                gsheet.update_cell(i, 21, '{:.2f}'.format(unrealized_pnl))
                gsheet.update_cell(i, 22, '{:.2%}'.format(unrealized_pnl_pct))
            elif sell_time:  # 如果卖出时间不为空
                gsheet.update_cell(i, 20, '')  
                gsheet.update_cell(i, 21, '')
                gsheet.update_cell(i, 22, '')
    return "Update completed successfully"

def find_previous_total(gsheet, row_number, initial_capital):
    while row_number > 1:  # 从当前行号开始向上遍历，直到第一行
        row_number -= 1  # 移动到上一行
        total_value = gsheet.cell(row_number, 17).value  # 尝试获取这一行的总资金值
        if total_value:  # 如果这一行有总资金值
            try:
                return float(total_value)  # 返回这个值转换成浮点数
            except ValueError:
                # 如果转换失败，继续向上遍历
                continue
    return initial_capital  # 如果所有上面的行都没有值，返回初始资本

def find_cell_safely(worksheet, value):
    try:
        return worksheet.find(value)
    except gspread.exceptions.CellNotFound:
        return None
    
@app.route('/add_tradelog', methods=['POST'])
def add_tradelog():
    data = request.get_json()
    initial_capital = 5000  # 初始资金设定为 5000

    strategy = data.get('strategy', 'Unknown Strategy')
    if strategy == "Break1h":
        gsheet = client.open("freqtrade tradelog").worksheet('BreakoutCatcher')
    elif strategy == "TrendCatcher":
        gsheet = client.open("freqtrade tradelog").worksheet('TrendCatcher')

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

        cell = find_cell_safely(gsheet, trade_id)

        if cell: #加仓
            row_number = cell.row
            current_stake = float(gsheet.cell(row_number, 10).value or 0)
            current_fees = float(gsheet.cell(row_number, 15).value or 0)
           
            if not gsheet.cell(row_number, 6).value: # 检查第6列是否为空
                gsheet.update_cell(row_number, 6, open_rate)
                gsheet.update_cell(row_number, 7, amount)
            else: # 否则更新第8列和第9列                
                gsheet.update_cell(row_number, 8, open_rate)
                gsheet.update_cell(row_number, 9, amount)

            total_stake = current_stake + float(data['stake_amount'])
            gsheet.update_cell(row_number, 10, '{:.2f}'.format(total_stake))
            gsheet.update_cell(row_number, 15, current_fees + fees)

        else: #首次
            row_data = [trade_id, pair, open_date, open_rate, amount] + [''] * 4 + [stake_amount] + [''] * 4 + [fees]
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
        profit_amount = float(data['profit_amount'])
        profit_ratio = '{:.2%}'.format(float(data['profit_ratio']))
        exit_reason = data.get('exit_reason', 'N/A')
        open_date = data.get('open_date', 'N/A')
        close_date_str = data.get('close_date', 'N/A')
        close_date_obj = datetime.strptime(close_date_str, '%Y-%m-%d %H:%M:%S.%f%z')
        close_date = close_date_obj.strftime('%Y-%m-%d %H:%M:%S')

        cell = gsheet.find(trade_id)
        if cell:
            row_number = cell.row
            # gsheet.update('G{}'.format(row_number), close_rate)  # 假设卖出时间对应第7列
            gsheet.update_cell(row_number, 11, close_date)
            gsheet.update_cell(row_number, 12, close_rate)  
            gsheet.update_cell(row_number, 13, amount)       
            gsheet.update_cell(row_number, 14, exit_reason)        
            gsheet.update_cell(row_number, 19, profit_ratio)   
            # fee
            existing_fees = float(gsheet.cell(row_number, 15).value) 
            new_fees = existing_fees + close_rate * amount * 0.00075
            gsheet.update_cell(row_number, 15, new_fees)
            # 总资金
            previous_total = find_previous_total(gsheet, row_number, initial_capital)
            new_total = previous_total + profit_amount
            gsheet.update_cell(row_number, 16, '{:.2f}'.format(profit_amount)) 
            gsheet.update_cell(row_number, 17, '{:.2f}'.format(new_total))
            # 更新PnL Ratio
            pnl_ratio = (profit_amount / new_total) * 100
            gsheet.update_cell(row_number, 18, '{:.2f}%'.format(pnl_ratio))
            # 更新WIN/LOSS
            win_loss = "WIN" if profit_amount > 0 else "LOSS"
            gsheet.update_cell(row_number, 23, win_loss)

            return jsonify({"message": "Trade logged successfully"})
        else:
            return jsonify({"error": "Trade ID not found"}), 404

    return jsonify({"message": "No new trades for now"}), 200
    
scheduler = BackgroundScheduler()
scheduler.add_job(func=update_prices, trigger="interval", minutes=5, next_run_time=datetime.now())
scheduler.start()

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=5000, use_reloader=False)