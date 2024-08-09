from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Create a mapping for representation status with numbered patterns
representation_status_map = {
    'no_representation': [
        (1, re.compile(r'^(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did\s*not\s*appear$', re.IGNORECASE)),
        (2, re.compile(r'^(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*was\s*not\s*represented$', re.IGNORECASE)),
        (3, re.compile(r'^(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did\s*not\s*appear\s*and\s*was\s*not\s*represented$', re.IGNORECASE)),
        (4, re.compile(r'^(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did\s*not\s*appear\s*and\s*were\s*not\s*represented$', re.IGNORECASE)),
        (5, re.compile(r'^(?P<party>[A-Za-z\s\'\-.]+)\s*\(not\s*appearing\)$', re.IGNORECASE)),
        (6, re.compile(r'The\s*(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*DID\s*NOT\s*APPEAR\s*AND\s*WAS\s*NOT\s*REPRESENTED$', re.IGNORECASE)),
    ],
    'in_person': [
        (7, re.compile(r'^(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*appeared\s*in\s*person$', re.IGNORECASE)),
        (8, re.compile(r'^(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*in\s*person$', re.IGNORECASE)),
        (9, re.compile(r'^The\s*(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*appeared\s*in\s*person$', re.IGNORECASE)),
    ],
    'representation': [  # This will cover the rest
        (10, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:,\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*,?\s*instructed\s*by\s*(?P<solicitor>[^,]+?)\s*,?\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (11, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:,\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*,?\s*instructed\s*by\s*(?P<solicitor>[^,]+?)\s*,?\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (12, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:,\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*,?\s*instructed\s*by\s*(?P<solicitor>[^,]+?)\s*,?\s*appeared\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (13, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:,\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*,?\s*instructed\s*by\s*(?P<solicitor>[^,]+?)\s*,?\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (14, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*,?\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*\(instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (15, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*,?\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*\(instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (16, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*,?\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*\(instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*appeared\s*for\s*the\s*(?P<party>[A-Za.z\s\'\-.]+)$',
            re.IGNORECASE)),
        (17, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*,?\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*\(instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za.z\s\'\-.]+)$',
            re.IGNORECASE)),
        (18, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*,?\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*\(Instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (19, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*,?\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*\(Instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (20, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*,?\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*\(Instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*appeared\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (21, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*,?\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*\(Instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (22, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*,?\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*instructed\s*by\s*(?P<solicitor>[^)]+?)\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (23, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*,?\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*instructed\s*by\s*(?P<solicitor>[^)]+?)\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (24, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*,?\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*instructed\s*by\s*(?P<solicitor>[^)]+?)\s*appeared\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (25, re.compile(
            r'(?P<barristers>[A-Z][a-zA-Z\s\'\-.]+(?:\s*,?\s*[A-Z][a-zA-Z\s\'\-.]+)*)\s*(?:\s*and\s*[A-Z][a-zA-Z\s\'\-.]+)*\s*instructed\s*by\s*(?P<solicitor>[^)]+?)\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$',
            re.IGNORECASE)),
        (26, re.compile(r'^(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*of\s*Counsel\s*appeared\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$', re.IGNORECASE)),
        (27, re.compile(r'^(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*of\s*Counsel\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za.z\s\'\-.]+)$', re.IGNORECASE)),
        (28, re.compile(r'^(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)$', re.IGNORECASE)),
        (29, re.compile(r'^(?P<party>[A-Za-z\s\'\-.]+):\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*of\s*Counsel,\s*pro\s*bono$', re.IGNORECASE)),
        (30, re.compile(r'^(?P<party>[A-Za-z\s\'\-.]+):\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*of\s*(?P<chambers>[A-Za-z\s\'\-.]+)$', re.IGNORECASE)),
        (31, re.compile(r'^For\s*the\s*(?P<party>[A-Za-z\s\'\-.]+):\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*of\s*Counsel\s*\(instructed\s*by\s*(?P<solicitor>[A-Za-z\s\'\-.]+)\)$', re.IGNORECASE)),
        (32, re.compile(r'^For\s*the\s*(?P<party>[A-Za-z\s\'\-.]+):\s*(?P<legal_representation>[A-Za-z\s\'\-.]+),\s*(?P<firm>[A-Za-z\s\'\-.]+)$', re.IGNORECASE)),
        (33, re.compile(r'^The\s*(?P<party>[A-Za-z\s\'\-.]+)\s*was\s*represented\s*by\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*of\s*counsel\s*and\s*(?P<solicitor>[A-Za-z\s\'\-.]+),\s*solicitor,\s*of\s*(?P<solicitor_firm>[A-Za-z\s\'\-.]+)$', re.IGNORECASE)),
        (34, re.compile(r'^The\s*(?P<party>[A-Za-z\s\'\-.]+)\s*was\s*represented\s*by\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*of\s*counsel,\s*instructed\s*by\s*the\s*(?P<solicitor>[A-Za-z\s\'\-.]+)$', re.IGNORECASE)),
        (35, re.compile(r'^(?P<party>[A-Za-z\s\'\-.]+)\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*(?P<barrister2>[A-Za-z\s\'\-.]+)\s*\(instructed\s*by\s*(?P<solicitor>[A-Za-z\s\'\-.]+)\)$', re.IGNORECASE)),
        (36, re.compile(r'^(?P<party>[A-Za-z\s\'\-.]+)\s*(?P<barrister>[A-Za.z\s\'\-.]+)\s*(?P<barrister2>[A-Za-z\s\'\-.]+)\s*\(instructed\s*by\s*(?P<solicitor>[A-Za.z\s\'\-.]+)\)\s*\(written\s*submissions\s*only\)\s*(?P<barrister3>[A-Za.z\s\'\-.]+)\s*\(instructed\s*by\s*(?P<solicitor2>[A-Za.z\s\'\-.]+)\)$', re.IGNORECASE))
    ]
}

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
    matches = []

    # Search for matches in each tag
    for tag in soup.find_all():
        text = tag.get_text(strip=True)
        for status, patterns in representation_status_map.items():
            for index, (pattern_num, pattern) in enumerate(patterns):
                match = pattern.fullmatch(text)  # Use fullmatch to ensure the entire tag content matches the pattern
                if match:
                    groups = match.groupdict()
                    groups['representation_status'] = status  # Add the representation status to groups
                    matches.append({
                        'tag': tag.name,
                        'text': text,
                        'groups': groups,
                        'pattern': pattern.pattern,  # Add the full pattern that was matched
                        'pattern_num': pattern_num  # Add the pattern number
                    })

    if matches:
        print("------------------------------------------------------")
        print(f"URI: {uri}")
        for match in matches:
            print(f"Matched Tag: <{match['tag']}> {match['text']} </{match['tag']}>")
            print(f"Groups: {match['groups']}")
            print(f"Pattern Matched: {match['pattern']} (Pattern Number: {match['pattern_num']})")

        match_count['matched'] += 1
    else:
        not_found_uris.append(uri)
        match_count['not_matched'] += 1

test_uris = ['/ewca/crim/2022/381', '/ewhc/ch/2021/2951', '/ewca/crim/2022/1818', '/ewhc/comm/2022/2702', '/ewhc/qb/2019/1263', '/ewhc/comm/2022/1512', '/ewhc/scco/2022/2663', '/ukftt/grc/2023/590', '/uksc/2022/1', '/ukut/tcc/2022/298']


# Example URIs to test - replace with actual valid URIs
# test_uris = [
#     "/ewhc/ch/2021/2951",
#     "/eat/2022/36",
#     "/ewhc/pat/2022/1345",
#     "/ewhc/qb/2022/1484",
#     "/ewca/civ/2009/860",
#     "/ewca/crim/2022/381",
#     "/ewca/crim/2022/1818",
#     "/ewhc/ch/2022/789",
#     "/ewcop/2020/7",
#     "/ewfc/2022/153",
#     "/ewfc/b/2024/18",
#     "/ewfc/2022/52",
#     "/ewhc/admin/2022/2143",
#     "/ewhc/ch/2023/173",
#     "/ewhc/ch/2023/220",
#     "/ewhc/ch/2020/1726",
#     "/ewhc/ch/2022/2924",
#     "/ewhc/ch/2022/1268",
#     "/ewhc/ch/2023/2312",
#     "/ewhc/ch/2024/505",
#     "/ewhc/ch/2023/2348",
#     "/ewhc/ch/2022/2973",
#     "/ewhc/ch/2021/3385",
#     "/ewhc/ipec/2014/2084",
#     "/ewhc/pat/2006/1344",
#     "/ewhc/ch/2024/347",
#     "/ewhc/ch/2022/1244",
#     "/ewhc/ch/2022/1610",
#     "/ewhc/fam/2005/247",
#     "/ewhc/ipec/2022/1320",
#     "/ewhc/kb/2024/1525",
#     "/ewhc/admin/2024/1207",
#     "/ewhc/admlty/2022/2858",
#     "/ewhc/ch/2023/1756",
#     "/ewhc/comm/2022/3272",
#     "/ewhc/comm/2022/2799",
#     "/ewhc/comm/2023/2877",
#     "/ewhc/comm/2023/711",
#     "/ewhc/comm/2022/2702",
#     "/ewhc/admin/2023/92",
#     "/ewhc/tcc/2023/2030",
#     "/ewhc/tcc/2022/1152",
#     "/ewhc/qb/2019/1263",
#     "/ewhc/admin/2022/1770",
#     "/ewhc/admin/2008/2788",
#     "/ewhc/admlty/2022/206",
#     "/ewhc/ch/2023/944",
#     "/ewhc/comm/2012/394",
#     "/ewhc/comm/2022/1512",
#     "/ewhc/comm/2022/219",
#     "/ewhc/admin/2022/1635",
#     "/ewhc/tcc/2017/29",
#     "/ewhc/tcc/2022/1814",
#     "/ewhc/scco/2022/2663",
#     "/ewhc/ipec/2022/652",
#     "/ewhc/scco/2022/1538",
#     "/ukait/2008/50",
#     "/ukftt/tc/2022/305",
#     "/ukftt/grc/2023/590",
#     "/ukftt/tc/2023/867",
#     "/ukpc/2010/14",
#     "/uksc/2022/1",
#     "/ukut/aac/2017/25",
#     "/ukut/aac/2022/102",
#     "/ukut/aac/2022/103",
#     "/ukut/iac/2020/127",
#     "/ukut/lc/2022/153",
#     "/ukut/tcc/2022/298",
#     "/ewhc/ch/2022/1419",
#     "/ewhc/admin/2019/3242",
#     "/ewhc/ch/2022/2428"
# ]

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
