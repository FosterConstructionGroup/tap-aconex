import os
import requests
from datetime import datetime
import xmltodict


session = requests.Session()


# constants
base_url = "https://au1.aconex.com/api"


def get_all_pages(
    resource, endpoint, resource_name, search_name, extra_query_string=""
):
    first = get_page(resource, endpoint, search_name, extra_query_string)
    total_results = int(first["@TotalResults"])
    pages = int(first["@TotalPages"])

    if total_results == 0:
        return []

    # If only 1 page then this array will just be empty
    data = [
        get_page(resource, endpoint, search_name, extra_query_string, page_number=p)[
            "SearchResults"
        ]
        for p in range(2, pages + 1)
    ]
    data.insert(0, first["SearchResults"])

    return [row for page in data for row in coerce_to_list(page[resource_name])]


def get_page(resource, endpoint, search_name, extra_query_string="", page_number=1):
    return get_generic(
        resource,
        endpoint,
        f"?search_type=paged&page_number={page_number}&page_size=500"
        + extra_query_string,
    )[search_name]


def get_generic(resource, endpoint, query_string=""):
    res = session.request(method="GET", url=f"{base_url}/{endpoint}{query_string}")
    res.raise_for_status()
    return xmltodict.parse(res.text)


date_format = "%Y-%m-%d"
datetime_format = "%Y-%m-%d %H:%M:%S"


def coerce_to_list(page):
    if type(page) is list:
        return page
    return [page]


def format_date(dt, format=datetime_format):
    return datetime.strftime(dt, format)


def parse_date(dt, format=date_format):
    return datetime.strptime(dt, format)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
