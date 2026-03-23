import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List

from geopy.distance import geodesic
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim


def load_cache(cache_path: Path) -> Dict[str, Any]:
    if not cache_path.exists():
        return {}
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_cache(cache_path: Path, cache: Dict[str, Any]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def detect_address_column(fieldnames: List[str]) -> str:
    lowered = {name.lower(): name for name in fieldnames}
    if "address" in lowered:
        return lowered["address"]
    # If the header doesn't contain an address-like column name, take the first column.
    return fieldnames[0]


def geocode_addresses(
    addresses: List[str],
    *,
    poi_lat: float,
    poi_lon: float,
    cache_path: Path,
    min_delay_seconds: float,
    user_agent: str,
) -> List[Dict[str, Any]]:
    geolocator = Nominatim(user_agent=user_agent)
    geocode_one = RateLimiter(geolocator.geocode, min_delay_seconds=min_delay_seconds)
    poi = (poi_lat, poi_lon)

    cache = load_cache(cache_path)
    results: List[Dict[str, Any]] = []

    for raw in addresses:
        address = (raw or "").strip()
        if not address:
            results.append(
                {
                    "input_address": raw,
                    "latitude": "",
                    "longitude": "",
                    "geocoded_address": "",
                    "distance_km": "",
                    "status": "empty",
                    "error": "",
                }
            )
            continue

        key = address.lower()
        if key in cache:
            cached = cache[key]
            if cached is None:
                results.append(
                    {
                        "input_address": address,
                        "latitude": "",
                        "longitude": "",
                        "geocoded_address": "",
                        "distance_km": "",
                        "status": "not_found",
                        "error": "",
                    }
                )
                continue

            lat = cached.get("latitude")
            lon = cached.get("longitude")
            distance_km = geodesic(poi, (lat, lon)).km if lat is not None and lon is not None else ""
            results.append(
                {
                    "input_address": address,
                    "latitude": lat,
                    "longitude": lon,
                    "geocoded_address": cached.get("geocoded_address", ""),
                    "distance_km": distance_km,
                    "status": "cached",
                    "error": "",
                }
            )
            continue

        try:
            location = geocode_one(address)
        except Exception as e:
            results.append(
                {
                    "input_address": address,
                    "latitude": "",
                    "longitude": "",
                    "geocoded_address": "",
                    "distance_km": "",
                    "status": "error",
                    "error": str(e),
                }
            )
            cache[key] = None
            continue

        if location is None:
            cache[key] = None
            results.append(
                {
                    "input_address": address,
                    "latitude": "",
                    "longitude": "",
                    "geocoded_address": "",
                    "distance_km": "",
                    "status": "not_found",
                    "error": "",
                }
            )
            continue

        lat = location.latitude
        lon = location.longitude
        distance_km = geodesic(poi, (lat, lon)).km

        cache[key] = {
            "latitude": lat,
            "longitude": lon,
            "geocoded_address": getattr(location, "address", "") or getattr(location, "raw", {}).get("display_name", ""),
        }

        results.append(
            {
                "input_address": address,
                "latitude": lat,
                "longitude": lon,
                "geocoded_address": cache[key]["geocoded_address"],
                "distance_km": distance_km,
                "status": "ok",
                "error": "",
            }
        )

    save_cache(cache_path, cache)
    return results


def read_input_csv(input_path: Path) -> List[str]:
    with input_path.open("r", encoding="utf-8", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        sniffer = csv.Sniffer()
        try:
            dialect = sniffer.sniff(sample)
        except csv.Error:
            dialect = csv.excel

        reader = csv.reader(f, dialect=dialect)
        rows = list(reader)
        if not rows:
            return []

        header = rows[0]
        # If the first row looks like a header (at least 2 columns and a likely address column).
        if any(h.lower().strip() in {"address", "street", "place"} for h in header) and len(header) >= 2:
            # Re-read using DictReader so we can pick the address column reliably.
            f.seek(0)
            dict_reader = csv.DictReader(f, dialect=dialect)
            fieldnames = dict_reader.fieldnames or header
            address_col = detect_address_column(fieldnames)
            return [(row.get(address_col) or "") for row in dict_reader]

        # Otherwise assume the file has one column and lines after the first are addresses.
        if len(rows[0]) == 1:
            return [r[0] for r in rows[1:] if r]
        # If the file is multi-column without a clear header, use the first column.
        return [r[0] for r in rows[1:] if r and len(r[0]) > 0]


def write_output_csv(output_path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "input_address",
        "latitude",
        "longitude",
        "geocoded_address",
        "distance_km",
        "status",
        "error",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Geocode addresses and compute distance.")
    parser.add_argument("--input", default="input.csv", help="Path to input CSV with addresses.")
    parser.add_argument("--output", default="output.csv", help="Path to output CSV.")
    parser.add_argument(
        "--user-agent",
        default="geopy_miniproject/1.0 (contact: you@example.com)",
        help="User-Agent for Nominatim (preferably with contact info).",
    )
    parser.add_argument("--poi-lat", type=float, default=55.7558, help="Latitude of point of interest.")
    parser.add_argument("--poi-lon", type=float, default=37.6173, help="Longitude of point of interest.")
    parser.add_argument("--min-delay-seconds", type=float, default=1.0, help="Delay between requests to geocoder.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    cache_path = Path("cache") / "geocode_cache.json"

    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path.resolve()}")

    addresses = read_input_csv(input_path)
    if not addresses:
        raise SystemExit("No addresses found in input CSV.")

    rows = geocode_addresses(
        addresses,
        poi_lat=args.poi_lat,
        poi_lon=args.poi_lon,
        cache_path=cache_path,
        min_delay_seconds=args.min_delay_seconds,
        user_agent=args.user_agent,
    )
    write_output_csv(output_path, rows)

    ok = sum(1 for r in rows if r.get("status") in {"ok", "cached"})
    print(f"Done. Geocoded: {ok}/{len(rows)}. Output: {output_path.resolve()}")


if __name__ == "__main__":
    main()

