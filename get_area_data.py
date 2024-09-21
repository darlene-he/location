import json
import random
import time
import requests

from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

session = requests.Session()

headers_list = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15"},
    {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"},
    {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Safari/605.1.15"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36"}
]


def fetch_page(url, retries=10, delay=5):
    for attempt in range(retries):
        try:
            headers = random.choice(headers_list)
            time.sleep(random.uniform(0.5, 5))
            response = session.get(url, headers=headers)
            response.encoding = 'utf-8'
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    print(f"Failed to fetch page after {retries} attempts.")
    return None


def get_provinces(base_url):
    url = f"{base_url}index.html"
    soup = fetch_page(url)
    if not soup:
        return {}
    provinces = {
        province.text.strip(): {
            'code': province['href'].split('.')[0],
            'label': "province",
            'name': province.text.strip(),
            'link': urljoin(base_url, province['href']),
            'cities': {}
        }
        for province in soup.select('.provincetr a')
    }
    for province in provinces.values():
        print(f"Fetched province: {province['name']}, {province['code']}, {province['label']}, {province['link']}")
    print("Provinces fetching completed.")
    return provinces


def fetch_city(province_name, province_info, provinces):
    city_soup = fetch_page(province_info['link'])
    if not city_soup:
        return
    for city_row in city_soup.select('.citytr'):
        city_links = city_row.find_all('a')
        if len(city_links) < 2:
            continue
        code, name = city_links[0].text.strip(), city_links[1].text.strip()
        link = urljoin(province_info['link'], city_links[1].get('href'))
        provinces[province_name]['cities'][name] = {
            'label': "city",
            'name': name,
            'code': code,
            'link': link,
            'counties': {}
        }
        print(f"Fetched city: {name}, {code}, city, {link}")


def get_cities(provinces):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_city, province_name, province_info, provinces) for province_name, province_info
                   in provinces.items()]
        for future in as_completed(futures):
            future.result()
    print("Cities fetching completed.")


def fetch_county(province_name, city_name, city_info, provinces):
    county_soup = fetch_page(city_info['link'])
    if not county_soup:
        return
    for county_row in county_soup.select('.countytr'):
        county_links = county_row.find_all('a') or county_row.find_all('td')
        if len(county_links) < 2:
            continue
        code, name = county_links[0].text.strip(), county_links[1].text.strip()
        link = urljoin(city_info['link'], county_links[1].get('href'))
        provinces[province_name]['cities'][city_name]['counties'][name] = {
            'label': "county",
            'name': name,
            'code': code,
            'link': link,
            'towns': {}
        }
        print(f"Fetched county: {name}, {code}, county, {link}")


def get_counties(provinces):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_county, province_name, city_name, city_info, provinces)
                   for province_name, province_info in provinces.items()
                   for city_name, city_info in province_info['cities'].items()]
        for future in as_completed(futures):
            future.result()
    print("Counties fetching completed.")


def fetch_town(province_name, city_name, county_name, county_info, provinces):
    town_soup = fetch_page(county_info['link'])
    if not town_soup:
        return
    for town_row in town_soup.select('.towntr'):
        town_links = town_row.find_all('a') or town_row.find_all('td')
        if len(town_links) < 2:
            continue
        code, name = town_links[0].text.strip(), town_links[1].text.strip()
        link = urljoin(county_info['link'], town_links[1].get('href'))
        provinces[province_name]['cities'][city_name]['counties'][county_name]['towns'][name] = {
            'label': "town",
            'name': name,
            'code': code,
            'link': link,
            'committees': {}
        }
        print(f"Fetched town: {name}, {code}, town, {link}")


def get_towns(provinces):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_town, province_name, city_name, county_name, county_info, provinces)
                   for province_name, province_info in provinces.items()
                   for city_name, city_info in province_info['cities'].items()
                   for county_name, county_info in city_info['counties'].items()]
        for future in as_completed(futures):
            future.result()
    print("Towns fetching completed.")


def fetch_committee(province_name, city_name, county_name, town_name, town_info, provinces):
    committee_soup = fetch_page(town_info['link'])
    if not committee_soup:
        return
    for committee_row in committee_soup.select('.villagetr'):
        committee_links = committee_row.find_all('td')
        if len(committee_links) < 3:
            continue
        code, town_code, name = committee_links[0].text.strip(), committee_links[1].text.strip(), committee_links[
            2].text.strip()
        provinces[province_name]['cities'][city_name]['counties'][county_name]['towns'][town_name]['committees'][
            name] = {
            'label': "committee",
            'name': name,
            'code': code,
            'town_code': town_code
        }
        print(f"Fetched committee: {name}, {code}, committee")


def get_committees(provinces):
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(fetch_committee, province_name, city_name, county_name, town_name, town_info, provinces)
            for province_name, province_info in provinces.items()
            for city_name, city_info in province_info['cities'].items()
            for county_name, county_info in city_info['counties'].items()
            for town_name, town_info in county_info['towns'].items()]
        for future in as_completed(futures):
            future.result()
    print("Committees fetching completed.")


def parseProvinces(provinces):
    return [
        {
            "label": province_info['label'],
            "name": province_name,
            "code": province_info.get("code", ""),
            "cities": [
                {
                    "label": city_info['label'],
                    "name": city_name,
                    "code": city_info.get("code", ""),
                    "counties": [
                        {
                            "label": county_info['label'],
                            "name": county_name,
                            "code": county_info.get("code", ""),
                            "towns": [
                                {
                                    "label": town_info['label'],
                                    "name": town_name,
                                    "code": town_info.get("code", ""),
                                    "committees": [
                                        {
                                            "label": committee_info['label'],
                                            "name": committee_name,
                                            "code": committee_info.get("code", "")
                                        }
                                        for committee_name, committee_info in town_info['committees'].items()
                                    ]
                                }
                                for town_name, town_info in county_info['towns'].items()
                            ]
                        }
                        for county_name, county_info in city_info['counties'].items()
                    ]
                }
                for city_name, city_info in province_info['cities'].items()
            ]
        }
        for province_name, province_info in provinces.items()
    ]


if __name__ == "__main__":
    base_url = "https://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/2023/"
    provinces = get_provinces(base_url)
    get_cities(provinces)
    get_counties(provinces)
    get_towns(provinces)
    get_committees(provinces)

    mapping = parseProvinces(provinces)
    with open('data/location.json', 'w', encoding='utf-8') as f:
        json.dump(mapping, f, ensure_ascii=False)

    from into_db import insert_location_data
    insert_location_data(mapping)
