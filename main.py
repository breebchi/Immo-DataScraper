from __future__ import annotations

import argparse
from pathlib import Path

import requests

from sources import ImmoWeltSource
from utils.output import write_csv, write_json
from utils.scrape_pipeline import collect_listing_urls, scrape_listings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape listings from real estate platforms.")
    subparsers = parser.add_subparsers(dest="source", required=True)

    immowelt = subparsers.add_parser("immowelt", help="Scrape immowelt.de listings.")
    immowelt.add_argument(
        "--search-url",
        help="Search URL template with optional {page} placeholder.",
    )
    immowelt.add_argument(
        "--pages",
        type=int,
        default=1,
        help="Number of search pages to fetch when using --search-url.",
    )
    immowelt.add_argument(
        "--listing-url",
        action="append",
        default=[],
        help="One listing URL to scrape (can be repeated).",
    )
    immowelt.add_argument(
        "--listing-urls-file",
        type=Path,
        help="Path to a text file with one listing URL per line.",
    )
    immowelt.add_argument(
        "--max-listings",
        type=int,
        default=0,
        help="Limit number of listings to scrape (0 means no limit).",
    )
    immowelt.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data"),
        help="Directory to write output JSON/CSV files.",
    )
    immowelt.add_argument(
        "--workers",
        type=int,
        default=6,
        help="Number of parallel workers for listing pages.",
    )
    immowelt.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay in seconds between requests.",
    )
    immowelt.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Retry count for 403/429/503 or network errors.",
    )
    immowelt.add_argument(
        "--cookie",
        help="Optional Cookie header value for authenticated requests.",
    )
    immowelt.add_argument(
        "--debug-html-dir",
        type=Path,
        help="Write raw HTML responses to this directory.",
    )

    return parser.parse_args()


def _load_listing_urls(args: argparse.Namespace) -> list[str]:
    urls = list(args.listing_url)
    if args.listing_urls_file:
        with open(args.listing_urls_file, "r", encoding="utf-8") as file:
            urls.extend(line.strip() for line in file if line.strip())
    return urls


def main() -> None:
    args = parse_args()
    source = ImmoWeltSource()

    listing_urls = _load_listing_urls(args)
    with requests.Session() as session:
        if not listing_urls:
            if not args.search_url:
                raise SystemExit("Provide --search-url or --listing-url to scrape.")
            search_urls = source.build_search_urls(args.search_url, args.pages)
            listing_urls = collect_listing_urls(
                source,
                search_urls,
                session,
                retries=args.retries,
                cookie=args.cookie,
                delay_s=args.delay,
            )

        if args.max_listings > 0:
            listing_urls = listing_urls[: args.max_listings]

        data = scrape_listings(
            source,
            listing_urls,
            session,
            max_workers=args.workers,
            retries=args.retries,
            cookie=args.cookie,
            delay_s=args.delay,
            debug_html_dir=args.debug_html_dir,
        )

    output_dir = args.output_dir
    json_path = output_dir / f"listings_{source.name}.json"
    csv_path = output_dir / f"listings_{source.name}.csv"
    write_json(data, json_path)
    write_csv(data, csv_path)
    print(f"Wrote {len(data)} listings to {json_path} and {csv_path}.")


if __name__ == '__main__':
    main()
