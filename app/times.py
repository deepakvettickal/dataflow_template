from datetime import datetime


def log(message):
    print(f'**************** {utc_now()} : {message}')


def utc_now():
    return datetime.utcnow()  # - timedelta(days=1)

