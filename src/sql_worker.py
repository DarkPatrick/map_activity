from metabase import Mb_Client
from dotenv import dotenv_values
import pandas as pd
import datetime
import math
import re
import geoip2.database
from concurrent.futures import ThreadPoolExecutor


class SqlWorker():
    def __init__(self) -> None:
        secrets: dict = dotenv_values(".env")

        self._mb_client: Mb_Client = Mb_Client(
            url=f"{secrets['mb_url']}",
            username=secrets["username"],
            password=secrets["password"]
        )   

    def get_query(self, query_name: str, params: dict = {}) -> str:
        sql_req: str = open(f"queries/{query_name}.sql").read()
        return sql_req.format(**params) if bool(params) else sql_req

    def get_payload(self, query: str) -> dict:
        payload: dict = {
            "database": 2,
            "type": "native",
            "format_rows": False,
            "pretty": False,
            "native": {
                "query": query
            }
        }
        return payload

    def convert_string_int2int(self, value: str) -> int:
        return int(value.replace(',', ''))

    def get_region(self, reader, ip):
        try:
            response = reader.city(ip)
            # return ip, response.country.name, response.subdivisions.most_specific.name
            return response.subdivisions.most_specific.name, response.location.latitude, response.location.longitude
        except geoip2.errors.AddressNotFoundError:
            # return ip, "Unknown", "Unknown"
            return 'Unknown', None, None

    def process_ip_addresses(self, ip_list, database_path, max_workers=10):
        results = []
        with geoip2.database.Reader(database_path) as reader:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self.get_region, reader, ip) for ip in ip_list]
                for future in futures:
                    results.append(future.result())
        return results

    def get_active_users(self) -> pd.DataFrame:
        query = self.get_query("users")
        payload = self.get_payload(query)
        query_result = self._mb_client.post("dataset/json", payload)
        df = pd.json_normalize(query_result)
        df.datetime = df.datetime.apply(self.convert_string_int2int)
        df.datetime = pd.to_datetime(df.datetime, unit='s').dt.tz_localize('UTC')
        df.platform = df.platform.apply(self.convert_string_int2int)
        df.is_bot = df.is_bot.apply(self.convert_string_int2int)
        df.user_cnt = df.user_cnt.apply(self.convert_string_int2int)
        df.to_csv('df_raw.csv')

        database_path = 'GeoLite2-City.mmdb'
        ip_addresses = df['ip_address'].tolist()
        results = self.process_ip_addresses(ip_addresses, database_path)
        # print(results)
        regions_df = pd.DataFrame(results, columns=['region', 'latitude', 'longitude'])
        df['region'] = regions_df['region']
        df['latitude'] = regions_df['latitude']
        df['longitude'] = regions_df['longitude']

        return df
