from pymongo import MongoClient

def create_indexes():
    client = MongoClient(host='localhost', port=27017)
    db = client.FrozenBacktest 
    collections = [
        'index_daily', 
        'stock_daily_real', 
        'stock_daily_hfq', 
        'stock_daily_limit',
        'stock_fundamental',
    ]
    for collection in collections:
        db[collection].create_index([('ts_code', 1), ('trade_date', 1)], background=True)
        print(f"Index created for {collection}")

if __name__ == '__main__':
    create_indexes()
