import requests
import csv
from time import sleep
import os

HEADERS = {
    "User-Agent": "Crossref2026GitHubBot/1.0 (mailto:your@email.com)"
}

MAX_ROWS_PER_FILE = 500_000

FIELDS = [
    "ARTICLE TITLE", "VOLUME", "ISSUE", "PAGE",
    "EP/EF", "E-FIRST COVERAGE",
    "JOURNAL TITLE", "PUBLISHER",
    "PERIOD COVER", "Logic",
    "Final_Volume", "Final_Issue"
]

def extract_date(item, field):
    parts = item.get(field, {}).get("date-parts")
    if parts:
        return "-".join(str(x) for x in parts[0])
    return None

def load_issns():
    with open("issn_list.txt") as f:
        return [i.strip() for i in f if i.strip()]

def open_new_csv(part_number):
    filename = f"crossref_2026_part_{part_number}.csv"
    f = open(filename, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=FIELDS)
    writer.writeheader()
    return f, writer, filename

def fetch_issn(issn, writer, state):
    cursor = "*"

    while True:
        url = (
            "https://api.crossref.org/works"
            f"?filter=issn:{issn},from-pub-date:2026-01-01,until-pub-date:2026-12-31"
            f"&rows=1000&cursor={cursor}"
        )

        r = requests.get(url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        msg = r.json()["message"]

        for item in msg["items"]:
            epub = extract_date(item, "published-online")
            print_date = extract_date(item, "published-print")

            if epub and print_date:
                ep_ef = "EP"
                logic = "Epub First – Final volume/issue assigned"
                final_volume = item.get("volume")
                final_issue = item.get("issue")
            elif epub:
                ep_ef = "EF"
                logic = "Epub First – Volume/Issue not assigned"
                final_volume = ""
                final_issue = ""
            else:
                ep_ef = "EP"
                logic = "Print only publication"
                final_volume = item.get("volume")
                final_issue = item.get("issue")

            writer.writerow({
                "ARTICLE TITLE": item.get("title", [""])[0],
                "VOLUME": item.get("volume"),
                "ISSUE": item.get("issue"),
                "PAGE": item.get("page"),
                "EP/EF": ep_ef,
                "E-FIRST COVERAGE": epub,
                "JOURNAL TITLE": item.get("container-title", [""])[0],
                "PUBLISHER": item.get("publisher"),
                "PERIOD COVER": "2026",
                "Logic": logic,
                "Final_Volume": final_volume,
                "Final_Issue": final_issue
            })

            state["row_count"] += 1

            if state["row_count"] >= MAX_ROWS_PER_FILE:
                state["file"].close()
                state["part"] += 1
                state["row_count"] = 0
                state["file"], state["writer"], fname = open_new_csv(state["part"])
                state["files"].append(fname)

        cursor = msg.get("next-cursor")
        if not cursor:
            break

        sleep(1)

def main():
    issns = load_issns()

    part = 1
    f, writer, fname = open_new_csv(part)

    state = {
        "file": f,
        "writer": writer,
        "row_count": 0,
        "part": part,
        "files": [fname]
    }

    for idx, issn in enumerate(issns, 1):
        print(f"[{idx}/{len(issns)}] Processing ISSN {issn}")
        fetch_issn(issn, state["writer"], state)

    state["file"].close()

    print("Generated files:")
    for file in state["files"]:
        print(" -", file)

if __name__ == "__main__":
    main()
