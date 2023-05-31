from collections import defaultdict

import argparse
import csv
import json
import os

from typing import Dict, List, Any


def file_exists(file_path: str) -> bool:
    return os.path.isfile(file_path)

def load_csv(file_path: str) -> List[Dict[str, str]]:
    data = []
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

def get_params() -> Dict[str, str]:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="C:/Users/SNEHA/PycharmProjects/input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="C:/Users/SNEHA/PycharmProjects/input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="C:/Users/SNEHA/PycharmProjects/input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="C:/Users/SNEHA/PycharmProjects/output_data/outputs/")
    return vars(parser.parse_args())


def load_json_lines(file_path: str) -> List[Dict[str, str]]:
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            data.append(json.loads(line))
    return data


def process_data(customers: List[Dict[str, str]], products: List[Dict[str, str]], transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    customer_map = {customer['customer_id']: customer for customer in customers}
    product_map = {product['product_id']: product for product in products}

    customer_purchase_counts = defaultdict(int)
    output_data = []

    for transaction in transactions:
        customer_id = transaction['customer_id']
        basket = transaction['basket']

        if customer_id in customer_map:
            loyalty_score = customer_map[customer_id]['loyalty_score']

            for item in basket:
                product_id = item['product_id']
                if product_id in product_map:
                    product_category = product_map[product_id]['product_category']
                    customer_purchase_counts[(customer_id, product_id, product_category)] += 1

    for (customer_id, product_id, product_category), purchase_count in customer_purchase_counts.items():
        output_data.append({
            'customer_id': customer_id,
            'loyalty_score': customer_map[customer_id]['loyalty_score'],
            'product_id': product_id,
            'product_category': product_category,
            'purchase_count': purchase_count
        })

    return output_data



def save_json(output_data: List[Dict[str, Any]], output_location: str) -> None:
    with open(output_location, 'w') as file:
        for item in output_data:
            file.write(json.dumps(item) + '\n')


def main():
    params = get_params()
    customers_file = params['customers_location']
    products_file = params['products_location']
    transactions_dir = params['transactions_location']
    output_dir = params['output_location']


    if not file_exists(customers_file) or not file_exists(products_file):
        print("Customers or products file does not exist.")
        return

    if not os.path.isdir(transactions_dir):
        print("Transactions directory does not exist.")
        return

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    customers = load_csv(customers_file)
    products = load_csv(products_file)
    for root, _, files in os.walk(transactions_dir):
        for file in _:
            for filename in os.listdir(os.path.join(transactions_dir,file)):
                # Check if the file is a JSON file
                if filename.endswith('.json'):
                    file_path = os.path.join(transactions_dir, file, filename)
                    transactions=load_json_lines(file_path)
                    output_data = process_data(customers, products, transactions)
                    save_json(output_data, params['output_location'] + "output.json")


if __name__ == "__main__":
    main()




