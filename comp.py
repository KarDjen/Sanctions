from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


# Function to count sentences in a given text
def count_sentences(text):
    return len(re.split(r'[.!?]', text))


# Function to fetch and parse legal representation
def fetch_and_print_tags(uri, not_found_uris, match_count):
    judgment_url = f"https://caselaw.nationalarchives.gov.uk{uri}/data.xml"
    try:
        response = requests.get(judgment_url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve judgment details for URI: {uri}. Error: {e}")
        not_found_uris.append(uri)
        match_count['not_matched'] += 1
        return

    soup = BeautifulSoup(response.content, 'xml')
    header_tag = soup.find('header')
    found_match = False

    if header_tag:
        keywords = ['appear', 'counsel', 'solicitor', 'instructed by', 'KC', 'QC', 'represented by', 'SC']
        pattern = re.compile('|'.join(keywords), re.IGNORECASE)

        # Find <p> tags with keywords in <header>
        p_tags_with_keywords = [p_tag.get_text(" ", strip=True) for p_tag in header_tag.find_all('p') if
                                pattern.search(p_tag.get_text())]

        if p_tags_with_keywords:
            print(f"URI: {uri}")
            for tag in p_tags_with_keywords:
                print(tag)
            print("------------------------------------------------------")
            match_count['matched'] += 1
            found_match = True
        else:
            # If no <p> tags found, search for other tags with keywords in <header>
            other_tags_with_keywords = []
            for tag in header_tag.find_all():
                if tag.name != 'p' and pattern.search(tag.get_text()):
                    other_tags_with_keywords.append(f"<{tag.name}> {tag.get_text(' ', strip=True)} </{tag.name}>")

            if other_tags_with_keywords:
                print(f"URI: {uri}")
                for tag in other_tags_with_keywords:
                    print(tag)
                print("------------------------------------------------------")
                match_count['matched'] += 1
                found_match = True

    if not found_match:
        # If no match found in <header>, search in <judgmentBody> with up to 3 sentences
        judgment_body_tag = soup.find('judgmentBody')
        if judgment_body_tag:
            body_tags_with_keywords = []
            for tag in judgment_body_tag.find_all():
                text = tag.get_text(" ", strip=True)
                if pattern.search(text) and count_sentences(text) <= 3:
                    body_tags_with_keywords.append(f"<{tag.name}> {text} </{tag.name}>")

            if body_tags_with_keywords:
                print(f"URI: {uri}")
                for tag in body_tags_with_keywords:
                    print(tag)
                print("------------------------------------------------------")
                match_count['matched'] += 1
            else:
                not_found_uris.append(uri)
                match_count['not_matched'] += 1
        else:
            not_found_uris.append(uri)
            match_count['not_matched'] += 1


# Example URIs to test - replace with actual valid URIs
test_uris = [
    "/ewhc/ch/2021/2951",
    "/eat/2022/36",
    "/ewhc/pat/2022/1345",
    "/ewhc/qb/2022/1484",
    "/ewca/civ/2009/860",
    "/ewca/crim/2022/381",
    "/ewca/crim/2022/1818",
    "/ewhc/ch/2022/789",
    "/ewcop/2020/7",
    "/ewfc/2022/153",
    "/ewfc/b/2024/18",
    "/ewfc/2022/52",
    "/ewhc/admin/2022/2143",
    "/ewhc/ch/2023/173",
    "/ewhc/ch/2023/220",
    "/ewhc/ch/2020/1726",
    "/ewhc/ch/2022/2924",
    "/ewhc/ch/2022/1268",
    "/ewhc/ch/2023/2312",
    "/ewhc/ch/2024/505",
    "/ewhc/ch/2023/2348",
    "/ewhc/ch/2022/2973",
    "/ewhc/ch/2021/3385",
    "/ewhc/ipec/2014/2084",
    "/ewhc/pat/2006/1344",
    "/ewhc/ch/2024/347",
    "/ewhc/ch/2022/1244",
    "/ewhc/ch/2022/1610",
    "/ewhc/fam/2005/247",
    "/ewhc/ipec/2022/1320",
    "/ewhc/kb/2024/1525",
    "/ewhc/admin/2024/1207",
    "/ewhc/admlty/2022/2858",
    "/ewhc/ch/2023/1756",
    "/ewhc/comm/2022/3272",
    "/ewhc/comm/2022/2799",
    "/ewhc/comm/2023/2877",
    "/ewhc/comm/2023/711",
    "/ewhc/comm/2022/2702",
    "/ewhc/admin/2023/92",
    "/ewhc/tcc/2023/2030",
    "/ewhc/tcc/2022/1152",
    "/ewhc/qb/2019/1263",
    "/ewhc/admin/2022/1770",
    "/ewhc/admin/2008/2788",
    "/ewhc/admlty/2022/206",
    "/ewhc/ch/2023/944",
    "/ewhc/comm/2012/394",
    "/ewhc/comm/2022/1512",
    "/ewhc/comm/2022/219",
    "/ewhc/admin/2022/1635",
    "/ewhc/tcc/2017/29",
    "/ewhc/tcc/2022/1814",
    "/ewhc/scco/2022/2663",
    "/ewhc/ipec/2022/652",
    "/ewhc/scco/2022/1538",
    "/ukait/2008/50",
    "/ukftt/tc/2022/305",
    "/ukftt/grc/2023/590",
    "/ukftt/tc/2023/867",
    "/ukpc/2010/14",
    "/uksc/2022/1",
    "/ukut/aac/2017/25",
    "/ukut/aac/2022/102",
    "/ukut/aac/2022/103",
    "/ukut/iac/2020/127",
    "/ukut/lc/2022/153",
    "/ukut/tcc/2022/298",
    "/ewhc/ch/2022/1419",
    "/ewhc/admin/2019/3242",
    "/ewhc/ch/2022/2428"
]

# Initialize counters and lists to track URIs
match_count = {'matched': 0, 'not_matched': 0}
not_found_uris = []

# Process URIs concurrently
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_and_print_tags, uri, not_found_uris, match_count): uri for uri in test_uris}
    for future in as_completed(futures):
        uri = futures[future]
        try:
            future.result()
        except Exception as e:
            print(f"Error processing URI {uri}: {e}")

# Print summary of results
print(f"Number of URIs where required tags were found: {match_count['matched']}")
print(f"Number of URIs where required tags were not found: {match_count['not_matched']}")
print("URIs where required tags were not found:")
print(f"Number of URIs: {len(not_found_uris)}")
print(not_found_uris)
