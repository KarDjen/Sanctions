from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


# Function to fetch and parse legal representation
def fetch_and_parse_legal_representation(uri, not_found_uris, match_count):
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
    representation = []

    # Compile patterns with case insensitivity and flexible spacing
    patterns = [
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*instructed\s*by\s*(?P<solicitor>[^,]+?)\s*,\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*instructed\s*by\s*(?P<solicitor>[^,]+?)\s*,\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*instructed\s*by\s*(?P<solicitor>[^,]+?)\s*,\s*appeared\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*instructed\s*by\s*(?P<solicitor>[^,]+?)\s*,\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*appeared\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(Instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(Instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(Instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*appeared\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(Instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*instructed\s*by\s*(?P<solicitor>[^)]+?)\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*instructed\s*by\s*(?P<solicitor>[^)]+?)\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*instructed\s*by\s*(?P<solicitor>[^)]+?)\s*appeared\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*instructed\s*by\s*(?P<solicitor>[^)]+?)\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*of\s*Counsel\s*appeared\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*of\s*Counsel\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*of\s*Counsel\s*appeared\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*appeared\s*in\s*person', re.IGNORECASE),
        re.compile(r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*in\s*person', re.IGNORECASE),
        re.compile(r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did\s*not\s*appear', re.IGNORECASE),
        re.compile(r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*were\s*not\s*represented', re.IGNORECASE),
        re.compile(r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*was\s*not\s*represented', re.IGNORECASE),
        re.compile(r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did\s*not\s*appear\s*and\s*was\s*not\s*represented', re.IGNORECASE),
        re.compile(r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did\s*not\s*appear\s*and\s*were\s*not\s*represented', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][A-Z\s\'\-.]+?)\s*solicitor\s*of\s*(?P<solicitor>[^ ]+?)\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][A-Z\s\'\-.]+?)\s*instructed\s*by\s*(?P<solicitor>[^\s]+?)\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'The\s*(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*DID\s*NOT\s*APPEAR\s*AND\s*WAS\s*NOT\s*REPRESENTED', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*of\s*Counsel\s*appeared\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'The\s*(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*appeared\s*in\s*person', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*and\s*(?P<barrister2>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed\s*by\s*(?P<solicitor>[^)]+?)\)\s*appeared\s*on\s*behalf\s*of\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'The\s*(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did\s*not\s*appear\s*and\s*was\s*not\s*represented', re.IGNORECASE),
        re.compile(r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did\s*not\s*appear\s*and\s*were\s*not\s*represented', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Za-z\s\'\-.]+),\s*instructed\s*by\s*(?P<solicitor>[A-Za-z\s\'\-.]+),\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Za-z\s\'\-.]+),\s*instructed\s*by\s*HMRC\s*solicitor,\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<party>[A-Za-z\s\'\-.]+)\s*\(not\s*appearing\)', re.IGNORECASE),
        re.compile(r'(?P<party>[A-Za-z\s\'\-.]+)\s*appeared\s*for\s*the\s*Claimant', re.IGNORECASE),
        re.compile(r'The\s*(?P<party>[A-Za-z\s\'\-.]+)\s*was\s*represented\s*by\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*of\s*counsel\s*and\s*(?P<solicitor>[A-Za-z\s\'\-.]+),\s*solicitor,\s*of\s*(?P<solicitor_firm>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'The\s*(?P<party>[A-Za-z\s\'\-.]+)\s*was\s*represented\s*by\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*of\s*counsel,\s*instructed\s*by\s*the\s*(?P<solicitor>[A-Za-z\s\'\-.]+)', re.IGNORECASE),
        re.compile(r'(?P<party>[A-Za-z\s\'\-.]+)\s*(?P<barrister>[A-Za-z\s\'\-.]+),\s*Counsel', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Za-z\s\'\-.]+),\s*Counsel', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Za-z\s\'\-.]+)\s*and\s*(?P<barrister2>[A-Za-z\s\'\-.]+),\s*Counsel', re.IGNORECASE),
        re.compile(r'(?P<barrister>[A-Za-z\s\'\-.]+),\s*instructed\s*by\s*(?P<solicitor>[A-Za-z\s\'\-.]+),\s*for\s*the\s*(?P<party>[A-Za-z\s\'\-.]+)', re.IGNORECASE)
    ]

    def extract_representation():
        header_tag = soup.find('header')
        judgment_body_tag = soup.find('judgmentBody')

        if header_tag:
            for tag in header_tag.find_all(re.compile('^(p|span|div|li|b|i|u|strong|em|a|td|th|tr|table|h[1-6])$', re.IGNORECASE)):
                text = tag.get_text(" ", strip=True)
                for pattern in patterns:
                    match = re.search(pattern, text)
                    if match:
                        add_to_representation(match, text, 'extract_representation')

        if judgment_body_tag:
            for p_tag in judgment_body_tag.find_all('p'):
                text = p_tag.get_text(" ", strip=True)
                for pattern in patterns:
                    match = re.search(pattern, text)
                    if match:
                        add_to_representation(match, text, 'extract_representation')

    def add_to_representation(match, text, source):
        barrister = match.group('barrister').strip() if 'barrister' in match.groupdict() else None
        solicitor = match.group('solicitor').strip() if 'solicitor' in match.groupdict() else None
        party = match.group('party').strip() if 'party' in match.groupdict() else None
        representation_status = (
            'in person' if 'in person' in text.lower() else
            'did not appear' if 'did not appear' in text.lower() else
            'not represented' if 'not represented' in text.lower() else
            None
        )

        representation.append({
            'barrister': barrister,
            'solicitor': solicitor,
            'party': party,
            'representation_status': representation_status,
            'source': source
        })

    extract_representation()

    # Remove duplicates
    unique_representation = [dict(t) for t in {tuple(d.items()) for d in representation}]

    if unique_representation:
        print(f"URI: {uri}")
        print(f"Representation: {unique_representation}")
        match_count['matched'] += 1

        # Create a custom tag with the extracted words
        representation_tag = "<extracted_representation>{}</extracted_representation>".format(
            ', '.join([
                "{}{} (party: {}) - {} - {}".format(
                    rep['barrister'] + " " if rep['barrister'] else "",
                    "(instructed by {})".format(rep['solicitor']) if rep['solicitor'] else "",
                    rep['party'],
                    rep['representation_status'],
                    rep['source']
                )
                for rep in unique_representation
            ])
        )
        print(f"Representation Custom Tag: {representation_tag}")
    else:
        print(f"URI: {uri}")
        print("No legal representation found.")
        not_found_uris.append(uri)
        match_count['not_matched'] += 1

    print("------------------------------------------------------")


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
    futures = {executor.submit(fetch_and_parse_legal_representation, uri, not_found_uris, match_count): uri for uri in
               test_uris}
    for future in as_completed(futures):
        uri = futures[future]
        try:
            future.result()
        except Exception as e:
            print(f"Error processing URI {uri}: {e}")

# Print summary of results
print(f"Number of URIs where legal representations were found: {match_count['matched']}")
print(f"Number of URIs where no legal representations were found: {match_count['not_matched']}")
print("URIs where no legal representations were found:")
print(f"Number of URIs: {len(not_found_uris)}")
print(not_found_uris)

from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


# Function to fetch and parse legal representation
def fetch_and_parse_legal_representation(uri, not_found_uris, match_count):
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
    representation = []

    patterns = [
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*instructed by (?P<solicitor>[^,]+?)\s*,\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*instructed by (?P<solicitor>[^,]+?)\s*,\s*on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*instructed by (?P<solicitor>[^,]+?)\s*,\s*appeared for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*instructed by (?P<solicitor>[^,]+?)\s*,\s*appeared on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed by (?P<solicitor>[^)]+?)\)\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed by (?P<solicitor>[^)]+?)\)\s*on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed by (?P<solicitor>[^)]+?)\)\s*appeared for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed by (?P<solicitor>[^)]+?)\)\s*appeared on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(Instructed by (?P<solicitor>[^)]+?)\)\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(Instructed by (?P<solicitor>[^)]+?)\)\s*on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(Instructed by (?P<solicitor>[^)]+?)\)\s*appeared for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(Instructed by (?P<solicitor>[^)]+?)\)\s*appeared on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*instructed by (?P<solicitor>[^)]+?)\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*instructed by (?P<solicitor>[^)]+?)\s*on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*instructed by (?P<solicitor>[^)]+?)\s*appeared for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*instructed by (?P<solicitor>[^)]+?)\s*appeared on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*of Counsel appeared for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*of Counsel appeared on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*of Counsel appeared\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*appeared in person',
        r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*in person',
        r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did not appear',
        r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*were not represented',
        r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*was not represented',
        r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did not appear and was not represented',
        r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did not appear and were not represented',
        r'(?P<barrister>[A-Z][A-Z\s\'\-.]+?)\s*solicitor of (?P<solicitor>[^ ]+?)\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][A-Z\s\'\-.]+?)\s*instructed by (?P<solicitor>[^\s]+?)\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed by (?P<solicitor>[^)]+?)\)\s*appeared on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'The\s*(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*DID NOT APPEAR AND WAS NOT REPRESENTED',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*of Counsel appeared for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'The\s*(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*appeared in person',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*appeared on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed by (?P<solicitor>[^)]+?)\)\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*and\s*(?P<barrister2>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed by (?P<solicitor>[^)]+?)\)\s*appeared on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'The\s*(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did not appear and was not represented',
        r'(?P<party>[A-Z][a-zA-Z\s\'\-.]+?)\s*did not appear and were not represented',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*(?P<barrister2>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*(?P<barrister3>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*(?P<barrister4>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*(?P<barrister5>[A-Z][a-zA-Z\s\'\-.]+?)\s*,\s*(?P<barrister6>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed by (?P<solicitor>[^)]+?)\)\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'NO APPEARANCE by or on behalf of the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed by (?P<solicitor>[^)]+?)\)\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)\s*,\s*(?P<barrister2>[A-Z][a-zA-Z\s\'\-.]+?)\s*\(instructed by (?P<solicitor2>[^)]+?)\)\s*for the\s*(?P<party2>[A-Za-z\s\'\-.]+)',
        r'For the\s*(?P<party>[A-Za-z\s\'\-.]+):\s*(?P<solicitor>[A-Za-z\s\'\-.]+),\s*Solicitors',
        r'For the\s*(?P<party>[A-Za-z\s\'\-.]+):\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*\(instructed by\s*(?P<solicitor>[A-Za-z\s\'\-.]+)\)',
        r'For the\s*(?P<party>[A-Za-z\s\'\-.]+):\s*(?P<legal_representation>[A-Za-z\s\'\-.]+),\s*litigator of HM Revenue and Customs’ Solicitor’s Office',
        r'For the\s*(?P<party>[A-Za-z\s\'\-.]+):\s*(?P<legal_representation>[A-Za-z\s\'\-.]+),\s*(?P<firm>[A-Za-z\s\'\-.]+)',
        r'(?P<party>[A-Za-z\s\'\-.]+)\s*:\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*of Counsel, pro bono',
        r'(?P<party>[A-Za-z\s\'\-.]+)\s*:\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*of\s*(?P<chambers>[A-Za-z\s\'\-.]+)',
        r'For the\s*(?P<party>[A-Za-z\s\'\-.]+):\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*of Counsel\s*\(instructed by\s*(?P<solicitor>[A-Za-z\s\'\-.]+)\)',
        r'(?P<party>[A-Za-z\s\'\-.]+)\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*(?P<barrister2>[A-Za-z\s\'\-.]+)\s*\(instructed by\s*(?P<solicitor>[A-Za-z\s\'\-.]+)\)',
        r'(?P<party>[A-Za-z\s\'\-.]+)\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*(?P<barrister2>[A-Za-z\s\'\-.]+)\s*\(instructed by\s*(?P<solicitor>[A-Za-z\s\'\-.]+)\)\s*\(written submissions only\)\s*(?P<barrister3>[A-Za-z\s\'\-.]+)\s*\(instructed by\s*(?P<solicitor2>[A-Za-z\s\'\-.]+)\)',
        r'For the\s*(?P<party>[A-Za-z\s\'\-.]+):\s*(?P<barrister>[A-Za-z\s\'\-.]+),\s*instructed by\s*(?P<solicitor>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Za-z\s\'\-.]+),\s*instructed by\s*(?P<solicitor>[A-Za-z\s\'\-.]+),\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<barrister>[A-Za-z\s\'\-.]+),\s*instructed by HMRC solicitor,\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)',
        r'(?P<party>[A-Za-z\s\'\-.]+)\s*\(not appearing\)',
        r'(?P<party>[A-Za-z\s\'\-.]+)\s*appeared for the Claimant',
        r'The\s*(?P<party>[A-Za-z\s\'\-.]+)\s*was represented by\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*of counsel and\s*(?P<solicitor>[A-Za-z\s\'\-.]+),\s*solicitor, of\s*(?P<solicitor_firm>[A-Za-z\s\'\-.]+)',
        r'The\s*(?P<party>[A-Za-z\s\'\-.]+)\s*was represented by\s*(?P<barrister>[A-Za-z\s\'\-.]+)\s*of counsel,\s*instructed by the\s*(?P<solicitor>[A-Za-z\s\'\-.]+)',
        r'(?P<party>[A-Za-z\s\'\-.]+)\s*(?P<barrister>[A-Za-z\s\'\-.]+),\s*Counsel',
        r'(?P<barrister>[A-Za-z\s\'\-.]+),\s*Counsel',
        r'(?P<barrister>[A-Za-z\s\'\-.]+)\s*and\s*(?P<barrister2>[A-Za-z\s\'\-.]+),\s*Counsel',
        r'(?P<barrister>[A-Za-z\s\'\-.]+),\s*instructed by\s*(?P<solicitor>[A-Za-z\s\'\-.]+),\s*for the\s*(?P<party>[A-Za-z\s\'\-.]+)'
    ]

    def extract_representation():
        header_tag = soup.find('header')
        judgment_body_tag = soup.find('judgmentBody')

        if header_tag:
            for tag in header_tag.find_all(re.compile('^(p|span|div|li|b|i|u|strong|em|a|td|th|tr|table|h[1-6])$')):
                text = tag.get_text(" ", strip=True)
                for pattern in patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        add_to_representation(match, text, 'extract_representation')

        if judgment_body_tag:
            for p_tag in judgment_body_tag.find_all('p'):
                text = p_tag.get_text(" ", strip=True)
                for pattern in patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        add_to_representation(match, text, 'extract_representation')

    def add_to_representation(match, text, source):
        barrister = match.group('barrister').strip() if 'barrister' in match.groupdict() else None
        solicitor = match.group('solicitor').strip() if 'solicitor' in match.groupdict() else None
        party = match.group('party').strip() if 'party' in match.groupdict() else None
        representation_status = (
            'in person' if 'in person' in text.lower() else
            'did not appear' if 'did not appear' in text.lower() else
            'not represented' if 'not represented' in text.lower() else
            None
        )

        representation.append({
            'barrister': barrister,
            'solicitor': solicitor,
            'party': party,
            'representation_status': representation_status,
            'source': source
        })

    extract_representation()

    # Remove duplicates
    unique_representation = [dict(t) for t in {tuple(d.items()) for d in representation}]

    if unique_representation:
        print(f"URI: {uri}")
        print(f"Representation: {unique_representation}")
        match_count['matched'] += 1

        # Create a custom tag with the extracted words
        representation_tag = "<extracted_representation>{}</extracted_representation>".format(
            ', '.join([
                "{}{} (party: {}) - {} - {}".format(
                    rep['barrister'] + " " if rep['barrister'] else "",
                    "(instructed by {})".format(rep['solicitor']) if rep['solicitor'] else "",
                    rep['party'],
                    rep['representation_status'],
                    rep['source']
                )
                for rep in unique_representation
            ])
        )
        print(f"Representation Custom Tag: {representation_tag}")
    else:
        print(f"URI: {uri}")
        print("No legal representation found.")
        not_found_uris.append(uri)
        match_count['not_matched'] += 1

    print("------------------------------------------------------")


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
    futures = {executor.submit(fetch_and_parse_legal_representation, uri, not_found_uris, match_count): uri for uri in
               test_uris}
    for future in as_completed(futures):
        uri = futures[future]
        try:
            future.result()
        except Exception as e:
            print(f"Error processing URI {uri}: {e}")

# Print summary of results
print(f"Number of URIs where legal representations were found: {match_count['matched']}")
print(f"Number of URIs where no legal representations were found: {match_count['not_matched']}")
print("URIs where no legal representations were found:")
print(f"Number of URIs: {len(not_found_uris)}")
print(not_found_uris)
