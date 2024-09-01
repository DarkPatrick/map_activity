import geoip2.database
from concurrent.futures import ThreadPoolExecutor


def get_region(reader, ip):
    try:
        response = reader.city(ip)
        return ip, response.country.name, response.subdivisions.most_specific.name
    except geoip2.errors.AddressNotFoundError:
        return ip, "Unknown", "Unknown"

def process_ip_addresses(ip_list, database_path, max_workers=10):
    results = []
    with geoip2.database.Reader(database_path) as reader:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(get_region, reader, ip) for ip in ip_list]
            for future in futures:
                results.append(future.result())
    return results

# Example usage
database_path = 'GeoLite2-City.mmdb'
ip_addresses = ['8.8.8.8', '1.1.1.1', '192.168.1.1', '10.0.0.1']

results = process_ip_addresses(ip_addresses, database_path)

for ip, country, region in results:
    print(f"IP: {ip}, Country: {country}, Region: {region}")