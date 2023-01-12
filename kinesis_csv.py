from traceback import format_exc
from decimal import Decimal
import csv
from sys import argv
from time import sleep
from requests import get

fields = [
    "created_at",
    "id",
    "type",
    "memo",
    "from",
    "to",
    "amount",
    "fee",
    "token",
    "successful",
]


def log(msg):
    with open(argv[2], 'a+') as f:
        f.write(f'{msg}\n')


def get_line(base, transaction_id):
    transaction = get(f'{base}/transactions/{transaction_id}').json()

    def fee_formatter(fee):
        return str('{0:.7f}'.format((Decimal(fee) * Decimal("0.0000001"))))

    token = 'kau' if 'kau-mainnet' in base else 'kau'
    memo = transaction.get('memo', '')
    fee_charged = fee_formatter(transaction["fee_charged"])

    url = f'{base}/transactions/{transaction_id}/operations?order=desc'
    ops = get(url).json()['_embedded']['records'][0]

    if ops['type'] == "create_account":
        return [
            ops["created_at"],
            transaction_id,
            ops["type"],
            memo,
            ops["source_account"],
            ops["account"],
            ops["starting_balance"],
            fee_charged,
            token,
            ops["transaction_successful"],
        ]

    if ops['type'] == "account_merge":
        effects = (get(
            f'{base}/transactions/{transaction_id}/effects?order=desc').
            json()['_embedded']['records'][1]
        )
        return [
            ops["created_at"],
            transaction_id,
            ops["type"],
            memo,
            ops["source_account"],
            ops["into"],
            effects["amount"],
            fee_charged,
            token,
            ops["transaction_successful"],
        ]

    if ops['type'] == "payment":
        return [
            ops["created_at"],
            transaction_id,
            ops["type"],
            memo,
            ops["source_account"],
            ops["to"],
            ops["amount"],
            fee_charged,
            token,
            ops["transaction_successful"],
        ]

    if ops['type'] == "inflation":
        try:
            amount = (get(
                f'{base}/transactions/{transaction_id}/effects?order=desc').
                json()['_embedded']['records'][0]
            )["amount"]
        except: # noqa
            # Sometimes amount is not set and that means it is 0
            # https://explorer.kinesis.money/transaction/KAG/7ffb646a28d988e81bfb25354dcfdcd48fac89aa69c2f33dcee539f2caaf9e2f
            amount = "0"

        return [
            ops["created_at"],
            transaction_id,
            ops["type"],
            memo,
            ops["source_account"],
            "",  # to is empty in this instance.
            amount,
            fee_charged,
            token,
            ops["transaction_successful"],
        ]

    if ops['type'] == "set_options":
        return [
            ops["created_at"],
            transaction_id,
            ops["type"],
            memo,
            ops["source_account"],
            "",  # to is empty in this instance.
            "",  # no amount
            fee_charged,
            token,
            ops["transaction_successful"],
        ]

    log(f"Failed to find type in {ops}")
    raise Exception(f"Failed to find type in {ops}")


def autoretry(message, times, func):
    error = "autotry not run, please set times>1"
    for _ in range(times):
        try:
            return func()
        except KeyboardInterrupt:
            log("KeyboardInterrupt")
            exit(-1)
        except: # noqa
            log("Failed. Sleeping 5 minutes and will try again")
            error = format_exc()
            sleep(60*5)

    log(f"Absolutely failed {message}\n{error}")
    raise Exception(f"Absolutely failed {message}\n{format_exc()}")


def scrape(base, fn):
    url = f"{base}/transactions?limit=100&order=desc"
    with open(fn, 'a+') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)

    run = True
    iterations = 1000000000000000000000
    while run:
        log(f'Scraping transaction list {url}')
        resp = autoretry(f"Get {url}", 10, lambda: get(url))
        url = resp.json()['_links']['next']['href']
        lines = []
        for record in resp.json()['_embedded']['records']:
            try:
                lines.append(autoretry(
                    f"get line {record}", 10,
                    lambda: get_line(base, record['id']),
                ))
            except KeyboardInterrupt:
                log("KeyboardInterrupt")
                exit(0)
            except: # noqa
                log(f"Failed record gathering. Restart with {url}")
                exit(-1)

        with open(fn, 'a') as csvfile:
            csvwriter = csv.writer(csvfile)
            for line in lines:
                csvwriter.writerow(line)

        iterations -= 1
        if iterations == 0:
            run = False
sleep(1)  
# Sleep for 1 second, as the stellar explorer has a limit of 3600 requests per hour

if len(argv) != 3:
    print("Please use python3 kinesis_csv.py output.csv logs.txt")
    exit(-1)

scrape("https://kag-mainnet.kinesisgroup.io", argv[1])
# scrape("https://kau-mainnet.kinesisgroup.io", "kinesis-kau-01.csv")
