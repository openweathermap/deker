import datetime
from concurrent.futures import ThreadPoolExecutor

import httpx

# route = "http://de1.owm.io:8000/history/1.0/nowcast/era5?start={start}&end={end}&lat=-0.1278692&lon=-178.4898423"
route = "http://fc15-backup-8.owm.io:1370/history/1.0/nowcast/era5?start={start}&end={end}&lat=85.1278692&lon=54.4898423"
# route = "http://fc15-backup-8.owm.io:8002/v1/collection/era5_nowcast/varray/by-primary/era5_nowcast/subset/[{start}:{end}, 4.50, -50.25, :]/data"
# route = "https://de1.owm.io:8000/v1/collection/era5_nowcast/varray/by-primary/era5_nowcast/subset/[{start}:{end}, 30, 60, :]/data"

client = httpx.Client(timeout=None, verify=False, http2=True)


def run(dt):
    url = route.format(start=float(dt), end=float(dt + 3600))
    res = client.get(url)
    print(res.status)
    return res.json()


if __name__ == "__main__":
    dts = range(1640995200, 1672531200, 3600)
    start = datetime.datetime.now()
    try:
        results = []
        for dt in dts:
            results.append(run(dt))
        # with ThreadPoolExecutor() as ex:
        #     results = ex.map(run, dts)
        # a = list(results)
        # print(a)
    finally:
        print(datetime.datetime.now() - start)

    # url = "http://fc15-backup-8.owm.io:1370/history/1.0/nowcast/era5?start=1675209600&end=1675209601&lat=15.1278692&lon=154.4898423"
    # start = datetime.datetime.now()
    # res = httpx.get(url).json()
    # print(datetime.datetime.now() - start)