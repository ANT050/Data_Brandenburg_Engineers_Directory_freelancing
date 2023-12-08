import requests
import html
import concurrent.futures
import pandas as pd
from fake_useragent import UserAgent
from typing import Optional, List
from bs4 import BeautifulSoup


def fetch_data_from_url(url: str, headers: dict) -> Optional[dict]:
    try:
        response = requests.post(url, headers=headers).json()

        return response

    except Exception as e:
        print(f"Ошибка: {e}")

        return None


def get_list_company_links(url: str, url_company: str, headers: dict) -> List[str]:
    response = fetch_data_from_url(url, headers)

    ids_list = [item["ident"] for item in response if "ident" in item]
    list_company_links = [f"{url_company}{i}" for i in ids_list]

    return list_company_links


def get_company_data(link: str, headers: dict) -> dict:
    url_company = fetch_data_from_url(link, headers)
    if url_company:
        list_item = url_company[0]
        anrede = list_item.get('anrede', "")
        titel = list_item.get('titel', "")
        vorname = list_item.get('vorname', "")
        nachname = list_item.get('nachname', "")

        company = list_item.get('firma', [])
        specialization = list_item.get('fachrichtung', [])
        activity = list_item.get('taetigkeit', [])
        company_description = list_item.get('description', "")
        description_text = html.unescape(company_description)

        company_data = {
            "Chamber of Engineering Expert": f"{anrede} {titel} {vorname} {nachname}",
            "Company": ', '.join(filter(None, map(str, company))),
            "City": list_item.get('ort', ""),
            "Postcode": list_item.get('plz', ""),
            "Address": list_item.get('str', ""),
            "Telephone": list_item.get('fon', ""),
            "Fax": list_item.get('fax', ""),
            "Website": list_item.get('web', ""),
            "email": list_item.get('email', ""),
            "Specialization": ', '.join(filter(None, map(str, specialization))),
            "Activity": ', '.join(filter(None, map(str, activity))),
            "Membership": list_item.get('mitgliedsart', ""),
            "Construction permit": list_item.get('bauvorlageberechtigt', ""),
            "Consulting Engineer": list_item.get('bi', ""),
            "Company Description": ''.join(BeautifulSoup(description_text, 'lxml').find_all(string=True))
        }

        return company_data


def process_all_links(url: str, url_company: str, headers: dict, threads: int) -> List[dict]:
    company_links = get_list_company_links(url, url_company, headers)
    data_all_companies = []
    count = 1
    with concurrent.futures.ThreadPoolExecutor(threads) as executor:
        futures = [executor.submit(get_company_data, link, headers) for link in company_links]

        for future in concurrent.futures.as_completed(futures):
            company_data = future.result()
            print(f"{count}. {company_data}")
            count += 1

            data_all_companies.append(company_data)

    return data_all_companies


def write_to_csv(data: list, filename: str) -> None:
    df = pd.DataFrame(data)
    df.columns = [
        "Chamber of Engineering Expert",
        "Company",
        "City",
        "Postcode",
        "Address",
        "Telephone",
        "Fax",
        "Website",
        "email",
        "Specialization",
        "Activity",
        "Membership",
        "Construction permit",
        "Consulting Engineer",
        "Company Description"
    ]
    df.to_csv(filename, index=False)


def main() -> None:
    headers = {"User-Agent": UserAgent().random}
    url_list_of_ids = 'https://www.bbik.de/typo3conf/ext/bbik_inka/Classes/Services/Inka/inka.php'
    url_company = 'https://www.bbik.de/typo3conf/ext/bbik_inka/Classes/Services/Inka/inka.php?action=member_info&inka_id='
    number_threads = 10

    all_company_data = process_all_links(url_list_of_ids, url_company, headers, number_threads)
    write_to_csv(all_company_data, 'output.csv')


if __name__ == '__main__':
    main()
