import requests

def get_res():
    res = {}
    try:
        r = requests.get(
            "https://www.vivino.com/api/wineries/235242/wines",
            params = {
                "per_page": "50",
                "include_all_vintages":"true",
                "language":"en",
            },
            headers= {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
            },
        )
        res = r.json()
    except Exception as e:
        print(e)
    return res

print(get_res())