import requests
import asyncio
from timeit import default_timer
from concurrent.futures import ThreadPoolExecutor

START_TIME = default_timer()

def get_request(session, i):
    # url = "https://api.dev.dxrx.io/profiles/v1/countries/?profileType=LAB"
    # url = "http://localhost:8080/profiles/v1/countries/?profileType=LAB"
    url = "http://localhost:8080/profiles/v1/labs/69018/tests"

    with session.get(url) as response:
        data = response.text

        if response.status_code != 200:
            print("FAILURE::{0}".format(url))

        elapsed_time = default_timer() - START_TIME
        completed_at = "{:5.2f}s".format(elapsed_time)
        print("{0:<30} {1:>20}".format(i, completed_at))
        return data

def post_request(session, i):
    # url = "https://api.dev.dxrx.io/profiles/v1/labs/volumes"
    url = "http://localhost:8080/profiles/v1/labs/volumes"

    with session.post(url, "{\"biomarkerIds\": [1],	\"diseaseIds\": [10],\"countryCode\": \"USA\",\"fromYear\": 2020,\"fromMonth\": 5,\"toYear\": 2020,	\"toMonth\": 6}") as response:
        data = response.text

        if response.status_code != 200:
            print("FAILURE::{0}".format(url))

        elapsed_time = default_timer() - START_TIME
        completed_at = "{:5.2f}s".format(elapsed_time)
        print("{0:<30} {1:>20}".format(i, completed_at))
        return data

async def start_async_process():
    print("{0:<30} {1:>20}".format("No", "Completed at"))
    with ThreadPoolExecutor(max_workers=100) as executor:
        with requests.Session() as session:
            session.headers = {"Content-Type": "application/json","Authorization": "Bearer "}

            loop = asyncio.get_event_loop()
            START_TIME = default_timer()
            tasks = [
                loop.run_in_executor(
                    executor,
                    post_request,
                    *(session,i)
                )
                for i in range(30)
            ]
            for response in await asyncio.gather(*tasks):
                pass


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(start_async_process())
    loop.run_until_complete(future)