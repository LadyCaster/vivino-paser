
from email.mime import base
import time
import requests
import re
import json
import asyncio
import aiohttp
import itertools
import pandas as pd

wine_types = {
    1: 'Red wine',
    2: 'White wine',
    3: 'Sparkling wine',
    4: 'Rosé',
    5: 'Dessert',
    6: 'Fortified'
}

failed_pages = []

with open('regions.json', 'r') as f:
        regions = json.load(f).get("regions")

with open('proxy_list.txt') as f:
    proxy_list = f.read().splitlines()

proxy = 'http://149.11.180.242:9999'

# with open('grapes.json', 'r') as f:
#         grapes = json.load(f).get("grapes")

country_codes = ['AU', 'RU', 'BE', 'BR', 'CA', 'DK', 'FR', 'DE', 'HK', 'IE', 'IT', 'JP', 'MK', 'NL', 'SG', 'ES', 'SE', 'CH', 'GB', 'US']


def parse_region_file(region_id):
    region = next((region for region in regions if region["id"] == region_id), {})
    country = region.get("country", {}).get("name")
    country_winaries_amount = region.get("country", {}).get("wineries_count")
    country_wines_count = region.get("country", {}).get("wines_count")
    country_grapes = region.get("country", {}).get("most_used_grapes", [])
    country_most_used_grapes = [grape.get("name") for grape in country_grapes]
    return {
        'region': region.get("name"),
        'country': country,
        "country_code": region.get("country", {}).get("code"),
        "country_winaries_amount": country_winaries_amount,
        "country_wines_count": country_wines_count,
        "country_most_used_grapes": country_most_used_grapes,
        }


async def get_similar_wines(session, wine_info):
    wine_id = wine_info.get('wine_id')
    avg_rating = wine_info.get('wine_avg_rating')
    price = wine_info.get('price')
    wine_style = wine_info.get('wine_style')
    wine_type = wine_info.get('wine_type')
    country_code = wine_info.get('country_code')
    async def get_res(rating):
        wines = []
        try:
            async with session.get(
                "https://www.vivino.com/api/explore/explore",
                params = {
                    "country_code": 'IT',
                    "country_codes": [country_code],
                    "currency_code": 'EUR',
                    "min_rating": f'{rating}',
                    "order_by":"ratings_average",
                    "order":"desc",
                    "price_range_max": f'{price + 5}',
                    "wine_style_ids": [wine_style] if wine_style else [],
                    "wine_type_ids": [wine_type],
                    "vc_only": 'true',
                    "page": 1,
                    "per_page": 25,
                    "language": "en"
                },
                proxy=proxy,
                headers= {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
                }
            ) as r:
                res = await r.json()
                wines = res.get("explore_vintage").get('matches')
        except Exception as e:
            print(e)
        return wines
    wines_list = []
    if avg_rating:
        res_matches = await get_res(avg_rating)
        matches_count = len(res_matches) if res_matches else 0
        if matches_count < 2:
            new_rating = avg_rating - 0.5
            res_matches = await get_res(new_rating)
        if res_matches:
            for match in res_matches:
                seo_name = match.get("vintage").get("wine", {}).get("seo_name", {})
                year = match.get("vintage", {}).get("year")
                match_wine_id = match.get("vintage", {}).get("wine", {}).get("id")
                price_id =  match.get("price", {}).get("amount"),
                if wine_id == match_wine_id:
                    continue
                url = f'https://www.vivino.com/AU/en/{seo_name}/w/{match_wine_id}?year={year}'
                wines_list.append({
                'name': match.get("vintage", {}).get("wine", {}).get("name", {}),
                'seo_name': seo_name,
                "wine_id": match_wine_id,
                "year": year,
                "price": price_id,
                "url": url
                })
    return {**wine_info, 'similar_wines': wines_list}

async def get_taste(session, wine_id):
    try:
        async with session.get(
            f'https://www.vivino.com/api/wines/{wine_id}/tastes?language=en',
            proxy=proxy,
            headers= {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
            }
        ) as r:
            res = await r.json()
            flavor = res.get("tastes", {}).get('flavor', [])
            structure = res.get("tastes", {}).get('structure', {})
            flavor_groups = []
            if flavor:
                for group in flavor:
                    flavor_groups.append({
                        'group_name': group.get('group', ''),
                        'mentioned':group.get('stats', {}).get('mentions_count', '')
                    })
    except Exception as e:
        structure = {}
        flavor_groups = []
    return {'wine_id': wine_id, "taste": {'structure': structure, 'flavor': flavor_groups}}

async def get_reviews(session, wine_id, reviews_count):
    reviews_list = []
    pages = reviews_count // 50 + 1 if reviews_count // 50 else 2
    for page in range(1, pages):
        try:
            async with session.get(
                f'https://www.vivino.com/api/wines/{wine_id}/reviews?per_page=50&page={page}&language=en',
                proxy=proxy,
                headers= {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
                }
            ) as r:
                res = await r.json()
                if res:
                    reviews_content = res.get("reviews", [])
                    reviews = []
                    for item in reviews_content:
                        review = {
                            "rating": item.get("rating"),
                            "note": item.get("note"),
                            "created_at": item.get("created_at")
                        }
                        reviews.append(review)
                    reviews_list.extend(reviews)
        except Exception as e:
            print(e)
    return {'wine_id': wine_id, 'reviews': reviews_list}

async def get_facts(session, wine_info, count):
    price_id = wine_info.get('price_id')
    wine_id = wine_info.get('wine_id')
    year = wine_info.get('year')
    seo_name = wine_info.get('seo_name')
    price_id = wine_info.get('price_id')
    base_url = f'https://www.vivino.com/IT/en/{seo_name}/w/{wine_id}?year={year}'
    url = f'{base_url}&price_id={price_id}' if price_id else base_url
    try:
        async with session.get(
            url,
            proxy=proxy,
            headers= {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
            }
        ) as r:
            res = await r.text()
            try:
                dataLayer = re.search(r'dataLayer = ([\s\S]*?);', res)
                layer_dict = json.loads(dataLayer.group(1).strip())
                wine_style = layer_dict[0].get('wine_style_id')
            except Exception as e:
                wine_style = ''
            try:
                prerender = re.search(r'window.__PRELOADED_STATE__.offerPageInformation = ([\s\S]*?)<', res)
                page_dict = json.loads(prerender.group(1).strip())
            except Exception as e:
                try:
                    prerender = re.search(r'window.__PRELOADED_STATE__.vintagePageInformation = ([\s\S]*?);', res)
                    page_dict = json.loads(prerender.group(1).strip())
                except Exception as e:
                    with open(f'pagefaults-IT.txt', 'a') as f:
                        f.write(f'\n{url}')
                    return
        vintages = page_dict.get("wine", {}).get("vintages", [])
        vintages_list = []
        for vintage in vintages:
            year = vintage.get('year', '')
            vintage_stat = vintage.get("statistics", {})
            vintage_reviews = vintage_stat.get('reviews_count')
            vintage_avg_rating = vintage_stat.get('ratings_average')
            vintages_list.append({
                "vintage_year": year,
                "vintage_reviews": vintage_reviews,
                "vintage_avg_rating": vintage_avg_rating
            })
        alcohol = page_dict.get("vintage", {}).get("wine_facts", {}).get('alcohol', '')
        wine_avg_rating = page_dict.get("vintage", {}).get("statistics", {}).get("ratings_average", '')
        drink_until = page_dict.get("vintage", {}).get("wine_facts", {}).get('drink_until', '')
        grapes = page_dict.get("vintage", {}).get('grapes', [])
        grapes_mapper = {grape.get('id'): grape.get('name') for grape in grapes} if grapes else {}
        composition = page_dict.get("vintage", {}).get('grape_composition', {})
        winery_wines_count = page_dict.get("vintage", {}).get("wine", {}).get('winery', {}).get("statistics", {}).get("wines_count")
        winery_avg_rating = page_dict.get("vintage", {}).get("wine", {}).get('winery', {}).get("statistics", {}).get("ratings_average")
        reviews_list = [vintage.get("vintage_reviews") for vintage in vintages_list if vintage]
        reviews_count = 0
        for item in reviews_list:
            reviews_count += int(item)
        return {
            **wine_info,
            "wine_style_id": wine_style,
            "vintages_list": vintages_list,
            "alcohol": alcohol,
            "wine_avg_rating": wine_avg_rating,
            "drink_until": drink_until,
            "grapes": grapes,
            "composition": composition,
            "winery_wines_count": winery_wines_count,
            "winery_avg_rating": winery_avg_rating,
            'url': url,
            "reviews_count": reviews_count
        }
    except Exception as e:
        return


async def get_wine_info(wine, count):
    seo_name = wine.get("vintage", {}).get("wine", {}).get("seo_name", {})
    wine_id = wine.get("vintage", {}).get("wine", {}).get("id")
    type_id = wine.get("vintage", {}).get("wine", {}).get("type_id")
    year = wine.get("vintage", {}).get("year")
    price_dict = wine.get("price", {})
    price_id = price_dict.get("id") if price_dict else ''
    price_amount = price_dict.get("amount") if price_dict else ''
    wine_type = wine_types.get(type_id)
    winery_id = wine.get("vintage", {}).get("wine", {}).get("winery", {}).get("id")
    region = wine.get("vintage", {}).get("wine", {}).get("region", {})
    region_id = region.get('id', '') if region else ''
    country_code = region.get("country", {}).get("code") if region else ''
    wine_info = {
        "wine_id": wine_id,
        "seo_name": seo_name,
        "year": year,
        "price_id": price_id,
        'name': wine.get("vintage", {}).get("wine", {}).get("name", {}),
        "wine_type": wine_type,
        'region_id': region_id,
        'winery': wine.get("vintage", {}).get("wine", {}).get("winery", {}).get("name", {}),
        'winery_id': winery_id,
        'price': price_amount,
        'country_code': country_code
    }    
    return wine_info

async def get_page(session, page_num):
    async with session.get("https://www.vivino.com/api/explore/explore",
        params = {
            "country_code": "IT",
            "currency_code":"EUR",
            "grape_filter":"varietal",
            "min_rating":"1",
            "order_by":"price",
            "order":"asc",
            "page": page_num,
        },
        proxy=proxy,
        headers= {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
        }) as response:
        res = await response.json()
        return res["explore_vintage"]["matches"]


async def get_wines_info(wines, loop):
    wines_info = await asyncio.gather(
        *[get_wine_info(wine, count) for count, wine in enumerate(wines) if wine],
    )
    return wines_info

async def get_wine_facts(wines, loop):
    connector = aiohttp.TCPConnector(ssl=False)
    timeout = aiohttp.ClientTimeout(3600)
    async with aiohttp.ClientSession(loop=loop, connector=connector, timeout=timeout) as session:
        wines_facts = await asyncio.gather(
            *[get_facts(session, wine, count) for count, wine in enumerate(wines) if wine],
        )
    return wines_facts

async def get_wines_similar(wines, loop):
    connector = aiohttp.TCPConnector(ssl=False)
    timeout = aiohttp.ClientTimeout(3600)
    async with aiohttp.ClientSession(loop=loop, connector=connector, timeout=timeout) as session:
        wines_similar = await asyncio.gather(
            *[get_similar_wines(session, wine) for wine in wines if wine],
        )
    return wines_similar

async def get_wines_reviews(wines, loop):
    connector = aiohttp.TCPConnector(ssl=False)
    timeout = aiohttp.ClientTimeout(3600)
    async with aiohttp.ClientSession(loop=loop, connector=connector, timeout=timeout) as session:
        wines_reviews = await asyncio.gather(
            *[get_reviews(session, wine.get('wine_id'), wine.get('reviews_count')) for wine in wines if wine],
        )
    return wines_reviews

async def get_wines_tastes(wines, loop):
    connector = aiohttp.TCPConnector(ssl=False)
    timeout = aiohttp.ClientTimeout(3600)
    async with aiohttp.ClientSession(loop=loop, connector=connector, timeout=timeout) as session:
        wines_tastes = await asyncio.gather(
            *[get_taste(session, wine.get('wine_id')) for wine in wines if wine],
        )
    return wines_tastes

async def get_rest_pages(pages, loop):
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(loop=loop, connector=connector) as session:
        matches = await asyncio.gather(
            *[get_page(session, page) for page in range(1, pages + 1)],
        )
    return list(itertools.chain.from_iterable(matches))

def get_first_page():
    def get_res():
        res = {}
        try:
            r = requests.get(
                "https://www.vivino.com/api/explore/explore",
                params = {
                    "country_code": "IT",
                    "currency_code":"EUR",
                    "grape_filter":"varietal",
                    "min_rating":"1",
                    "order_by":"price",
                    "order":"asc",
                    "page": 1,
                },
                headers= {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
                },
                proxies={
                    'http': '149.11.180.242:9999',
                    'https': '149.11.180.242:9999'
                }
            )
            res = r.json()
        except Exception as e:
            print(e)
        return res

    loop = asyncio.get_event_loop()
    res = get_res()
    records_count = res["explore_vintage"]["records_matched"]
    first_page_matches = res["explore_vintage"]["matches"]
    pages = records_count // 25 + 1
    wines = []
    wines = loop.run_until_complete(
        get_rest_pages(pages, loop))
    wines.extend(first_page_matches)
    wines_info = loop.run_until_complete(
            get_wines_info(wines, loop))
    wines_facts = loop.run_until_complete(get_wine_facts(wines_info, loop))
    similar_wines = loop.run_until_complete(get_wines_similar(wines_facts, loop))
    unique_ids = set([item['wine_id'] for item in similar_wines])
    unique_wines = [val for val in similar_wines if val['wine_id'] not in unique_ids]
    wines_reviews = loop.run_until_complete(get_wines_reviews(unique_wines, loop))
    wines_tastes = loop.run_until_complete(get_wines_tastes(unique_wines, loop))
    for item in similar_wines:
        for elem in wines_reviews:
            if item['wine_id'] == elem['wine_id']:
                item['reviews'] = elem['reviews']
        for elem in wines_tastes:
            if item['wine_id'] == elem['wine_id']:
                item['taste'] = elem['taste']
    for i, chunk in enumerate([similar_wines[i:i + 5000] for i in range(0, len(similar_wines), 5000)]):
        nornalized_data = pd.json_normalize(chunk, max_level=0)
        nornalized_data.to_csv(f'IT-{i}_csv.csv')
    # with open('wines_BE.json', 'w') as f:
    #     json.dump(wines_dict, f, indent=2, ensure_ascii=False)
    
get_first_page()