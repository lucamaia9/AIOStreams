#!/usr/bin/env python3
"""
Test Go intake_filter vs Python classifier consistency.

This script tests that the Go intake_filter matches the Python smart_hint
classification logic. They should agree on what to reject.

Usage:
    python3 test_go_python_consistency.py [--sample N] [--verbose]

Output:
    - Match rate: % of torrents where Go and Python agree
    - Discrepancies: cases where they disagree (needs investigation)
"""

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from lib.classifier_runtime import bootstrap_classifier_runtime

bootstrap_classifier_runtime(__file__)

from bitmagnet_smart_hint import classify_payload

# Go intake filter patterns (must match intake_filter.go)
ADULT_CODES = {
    "xxx", "porn", "av", "jav", "hentai", "nhentai", "xvideos", "xhamster",
    "pornhub", "redtube", "youporn", "brazzers", "naughty", "bangbros",
    "realitykings", "mofos", "fakehub", "digital playground", "private",
    "dorcel", "marc dorcel", "evil angel", "blacked", "vixen", "tushy",
    "deeper", "naughty america", "passion-hd", "loveherfeet", "jules jordan",
    "kink", "hardx", "eroticage", "family strokes", "fuckedhard18",
    "girls do porn", "girlsdoporn", "exploitedcollegegirls", "backroom",
    "castingcouch", "netvideogirls", "czech casting", "czechcasting",
    "czechav", "czech amateurs", "czech massage", "czech sauna",
    "czech swing", "czech bunker", "czech couples", "czech lesbians",
    "czech mega swingers", "czech orgy", "czech parties", "czech solarium",
    "czech taxes", "czech toilets", "czech wife swap",
}

ADULT_BRANDS = {
    "brazzers", "naughty america", "bangbros", "reality kings", "mofos",
    "fake hub", "digital playground", "private", "dorcel", "marc dorcel",
    "evil angel", "blacked", "vixen", "tushy", "deeper", "kink",
    "hardx", "eroticage", "family strokes", "love her feet", "jules jordan",
    "passion-hd", "ftv girls", "ftvgirls", "metart", "met art", "hegre",
    "x-art", "xart", "wow girls", "wowgirls", "little caprice", "nubiles",
    "teen mega world", "team skeet", "teamskeet", "mofos", "babes",
    "twistys", "twistys hard", "als scan", "alscan", "femjoy",
    "als angels", "alsangels", "in the crack", "inthecrack",
    "only tease", "onlytease", "art lingerie", "artlingerie",
    "cosmid", "pinup files", "pinupfiles", "scoreland", "danni",
    "ddfbusty", "busty cafe", "bustycafe", "big tits round asses",
    "bigtitsroundasses", "milf hunter", "milfhunter", "captain stabbin",
    "captainstabbin", "8th street latinas", "8thstreetlatinas",
    "mike in brazil", "mikeinbrazil", "cum fiesta", "cumfiesta",
    "we live together", "welivetogether", "mike's apartment", "mikesapartment",
    "round and brown", "roundandbrown", "money talks", "moneytalks",
    "pure 18", "pure18", "first time auditions", "firsttimeauditions",
    "dangerous dongs", "dangerousdongs", "huge naturals", "hugenaturals",
    "big naturals", "bignaturals", "street blowjobs", "streetblowjobs",
    "monster curves", "monstercurves", "moms bang teens", "momsbangteens",
    "moms lick teens", "momslickteens", "mommys girl", "mommysgirl",
    "girls way", "girlsway", "web young", "webyoung", "mom xxx", "momxxx",
}

ADULT_PERFORMERS = {
    "lisa ann", "riley reid", "mia khalifa", "sasha grey", "aspen rae",
    "jenna jameson", "tori black", "lexi belle", "bonnie rotten",
    "stormy daniels", "jenna haze", "jessica drake", "kirsten price",
    "alexis texas", "jada stevens", "kelsi monroe", "valentina nappi",
    "angela white", "nicolette shea", "brandi love", "alex chance",
    "eva lovia", "jillian janson", "morgan lee", "kendra lust",
    "nikki benz", "peta jensen", "rachel roxxx", "shyla stylez",
    "alex grey", "elsa jean", "hannah hays", "kenzie reeves",
    "lena paul", "lily rader", "maya bijou", "naomi woods",
    "nina north", "riley star", "rosalyn sphinx", "sophia leone",
    "vina sky", "whitney wright", "gianna michaels", "julia ann",
    "brandi love", "austin kincaid", "devon lee", "diamond foxxx",
    "dyanna lauren", "emma starr", "helly mae hellfire", "holly halston",
    "janet mason", "jordan kingsley", "kasey grant", "kendra secrets",
    "lauren vaughn", "lezley zen", "lisa ann", "makayla cox",
    "monique fuentes", "nikki sexx", "persia pele", "phoenix marie",
    "Raquel devine", "rhylee richards", "rhyse richards", "savannah stern",
    "shayla leveaux", "shayla stylez", "sienna west", "sky taylor",
    "taylor wane", "veronica avluv", "victoria valentine", "violett",
}

SCENE_RELEASE_PATTERNS = ["xxx", "mp4-xxx", "xxx-sd", "xxx-2160p", "xxx-1080p", "xxx-720p", "xxx-480p"]
EXPLICIT_PATTERNS = ["cum", "fuck", "anal", "blowjob", "hardcore", "dick", "cock", "pussy", "slut", "whore", "milf", "teen sex", "sex tape", "sex scene", "orgasm", "squirt", "creampie", "gangbang", "threesome", "lesbian sex", "oral sex", "oral"]
MIN_SIZE = 50 * 1024 * 1024  # 50MB minimum


def go_intake_filter(name: str, size: int) -> tuple[bool, str]:
    """
    Python implementation of Go intake_filter.ShouldReject.
    Returns (should_reject, reason) matching Go logic.
    """
    name_lower = name.lower()
    
    # Size check
    if size < MIN_SIZE:
        return True, "size_below_threshold"
    
    # Adult code check (e.g., "ABP-123", "SSIS-456")
    import re
    adult_code_pattern = r'\b([A-Z]{2,6}-?\d{2,5})\b'
    match = re.search(adult_code_pattern, name)
    if match:
        code = match.group(1).upper()
        prefix = code.split('-')[0] if '-' in code else code[:3]
        adult_prefixes = {
            "ABP", "ABW", "ABBA", "ABS", "ADN", "AF", "AUKG", "BKyn", "BLK", "BMF",
            "CA", "CEAD", "CESD", "CMA", "CR", "CVD", "DANDY", "DASD", "DDH", "DLDSS",
            "DVA", "EBOD", "EVO", "FAA", "FCH", "FHD", "FSDSS", "GANA", "GVG", "HAWA",
            "HBAD", "HND", "HONB", "HTHD", "HUN", "IBW", "IPX", "IPZZ", "JUFD", "JUFE",
            "JUKD", "JUFD", "JUL", "JUV", "KBVR", "KIRE", "KV", "LULU", "LUXU", "MADM",
            "MIAA", "MIDE", "MIUM", "MKCK", "MKMP", "MMND", "MNX", "MUKD", "MXGS", "NACX",
            "NACX", "NAZO", "NDK", "NHDTA", "NIKI", "NKDC", "NOBK", "NPD", "NSPS", "NSTA",
            "NT", "NV", "NXG", "OBA", "OYC", "PACO", "PARATH", "PENIS", "PRED", "PRTD",
            "RKI", "ROE", "ROYD", "RTP", "SAME", "SAN", "SDAB", "SDAM", "SDDE", "SDJS",
            "SDMM", "SDMU", "SDNM", "SDTH", "SDXM", "SES", "SHOT", "SIN", "SION", "SJCD",
            "SKMJ", "SKSK", "SLI", "SMBD", "SONE", "SQTE", "SSIS", "SSNI", "STARS", "STARS",
            "SUN", "SW", "T28", "TCD", "TIKM", "TOIN", "TPPN", "TRA", "UBE", "URE", "VEC",
            "VENX", "VEMA", "VENU", "VOSS", "WAAA", "WANZ", "XBDB", "XNIS", "XV", "ZOOO",
            "ZTA", "BDA", "BDSR", "BNDV", "BOMN", "BONU", "BT", "CENB", "CESD", "CETD",
            "CGD", "CGSS", "CJOD", "CLKI", "CMD", "CND", "CP", "CWM", "DASD", "DBER",
            "DCV", "DD", "DDHP", "DDHSS", "DDK", "DDSS", "DEL", "DEM", "DFE", "DFT",
            "DKTM", "DKY", "DOL", "DOTT", "DPH", "DVA", "DVDES", "DVR", "EBOD", "EF",
            "EKDV", "EL", "ERO", "ET", "ETQT", "EXB", "FSDSS", "FSDSS", "GANA", "GAS",
            "GC", "GL", "GLOB", "GLOR", "GLORY", "GM", "GO", "GOA", "GOD", "GRD", "GS",
            "GSM", "GT", "GVI", "GVI", "GVR", "HAWA", "HAWG", "HBAD", "HBB", "HBL",
            "HBU", "HD", "HDCL", "HGR", "HKD", "HL", "HLD", "HND", "HNDS", "HNJO",
            "HNU", "HODV", "HONB", "HONA", "HOT", "HP", "HPI", "HPJ", "HR", "HRE",
            "HRO", "HRR", "HRS", "HRT", "HRV", "HSH", "HSHR", "HSR", "HSRT", "HTHD",
            "HUN", "HUNTA", "HV", "HWA", "HYZD", "IENE", "IK", "IKST", "IPX", "IPZZ",
            "JAB", "JAC", "JAD", "JAM", "JAV", "JAX", "JAZ", "JBS", "JDD", "JDDJ",
            "JDF", "JDG", "JDGH", "JDI", "JDJ", "JDK", "JDKN", "JDL", "JDM", "JDN",
            "JDO", "JDP", "JDQ", "JDR", "JDS", "JDU", "JDV", "JDW", "JDX", "JDY",
            "JDZ", "JED", "JEM", "JES", "JEV", "JEZ", "JFC", "JFK", "JFN", "JFO",
            "JFP", "JFR", "JFS", "JFT", "JFU", "JFV", "JFW", "JFX", "JFY", "JFZ",
            "JG", "JGA", "JGB", "JGC", "JGD", "JGE", "JGF", "JGG", "JGH", "JGI",
            "JGJ", "JGK", "JGL", "JGM", "JGN", "JGO", "JGP", "JGQ", "JGR", "JGS",
            "JGT", "JGU", "JGV", "JGW", "JGX", "JGY", "JGZ", "JH", "JHA", "JHB",
            "JHC", "JHD", "JHE", "JHF", "JHG", "JHH", "JHI", "JHJ", "JHK", "JHL",
            "JHM", "JHN", "JHO", "JHP", "JHQ", "JHR", "JHS", "JHT", "JHU", "JHV",
            "JHW", "JHX", "JHY", "JHZ", "JI", "JIA", "JIB", "JIC", "JID", "JIE",
            "JIF", "JIG", "JIH", "JII", "JIJ", "JIK", "JIL", "JIM", "JIN", "JIO",
            "JIP", "JIQ", "JIR", "JIS", "JIT", "JIU", "JIV", "JIW", "JIX", "JIY",
            "JIZ", "JJ", "JJA", "JJB", "JJC", "JJD", "JJE", "JJF", "JJG", "JJH",
            "JJI", "JJJ", "JJK", "JJL", "JJM", "JJN", "JJO", "JJP", "JJQ", "JJR",
            "JJS", "JJT", "JJU", "JJV", "JJW", "JJX", "JJY", "JJZ",
        }
        if prefix in adult_prefixes:
            return True, "adult_code"
    
    # Adult brand check
    for brand in ADULT_BRANDS:
        if brand in name_lower:
            return True, "adult_brand"
    
    # Adult performer check
    for performer in ADULT_PERFORMERS:
        if performer in name_lower:
            return True, "adult_performer"
    
    # Explicit pattern check
    for pattern in EXPLICIT_PATTERNS:
        if pattern in name_lower:
            return True, "explicit_pattern"
    
    # Scene release check
    for pattern in SCENE_RELEASE_PATTERNS:
        if pattern in name_lower:
            return True, "scene_release"
    
    return False, ""


def python_smart_hint(name: str, size: int) -> tuple[bool, str]:
    """
    Test what Python smart_hint would decide.
    Returns (should_reject, reason).
    """
    try:
        decision = classify_payload(
            {
                "torrentId": 0,
                "infoHash": "0" * 40,
                "name": name,
                "size": size,
                "publishedAt": "",
                "files": [{"path": name, "size": size}],
            }
        )
        return bool(decision.get("reject", False)), str(
            decision.get("rejectReason") or ""
        )
    except Exception as e:
        return True, f"error: {e}"


def test_consistency(sample_size: int = 10000, verbose: bool = False):
    """
    Test Go vs Python consistency on sample torrents from database.
    """
    print(f"Testing Go intake_filter vs Python smart_hint consistency")
    print(f"Sample size: {sample_size}")
    print()
    
    # Connect to BitMagnet database
    conn = sqlite3.connect("postgresql://postgres:postgres@bitmagnet-postgres:5432/bitmagnet")
    
    # Get random sample of torrents
    cursor = conn.execute(f"""
        SELECT name, size FROM torrents
        WHERE size >= {MIN_SIZE}
        ORDER BY RANDOM()
        LIMIT {sample_size}
    """)
    
    results = {
        "total": 0,
        "go_reject": 0,
        "python_reject": 0,
        "both_reject": 0,
        "both_accept": 0,
        "discrepancies": [],
    }
    
    for name, size in cursor:
        results["total"] += 1
        
        go_reject, go_reason = go_intake_filter(name, size)
        py_reject, py_reason = python_smart_hint(name, size)
        
        if go_reject:
            results["go_reject"] += 1
        if py_reject:
            results["python_reject"] += 1
        
        if go_reject and py_reject:
            results["both_reject"] += 1
        elif not go_reject and not py_reject:
            results["both_accept"] += 1
        else:
            # Discrepancy
            results["discrepancies"].append({
                "name": name[:100],
                "size": size,
                "go_reject": go_reject,
                "go_reason": go_reason,
                "py_reject": py_reject,
                "py_reason": py_reason,
            })
        
        if verbose and results["total"] % 1000 == 0:
            print(f"Processed {results['total']} torrents...")
    
    conn.close()
    
    # Print results
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Total tested: {results['total']}")
    print(f"Go rejected: {results['go_reject']} ({100*results['go_reject']/results['total']:.1f}%)")
    print(f"Python rejected: {results['python_reject']} ({100*results['python_reject']/results['total']:.1f}%)")
    print()
    
    agree = results["both_reject"] + results["both_accept"]
    disagree = len(results["discrepancies"])
    match_rate = 100 * agree / results["total"]
    
    print(f"Agreement rate: {match_rate:.2f}% ({agree}/{results['total']})")
    print(f"  - Both reject: {results['both_reject']}")
    print(f"  - Both accept: {results['both_accept']}")
    print(f"Discrepancies: {disagree}")
    
    if results["discrepancies"]:
        print("\n" + "=" * 60)
        print("SAMPLE DISCREPANCIES (first 20)")
        print("=" * 60)
        for d in results["discrepancies"][:20]:
            print(f"\nName: {d['name']}")
            print(f"  Go: reject={d['go_reject']} ({d['go_reason']})")
            print(f"  Py: reject={d['py_reject']} ({d['py_reason']})")
    
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test Go vs Python consistency")
    parser.add_argument("--sample", type=int, default=10000, help="Sample size")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    test_consistency(args.sample, args.verbose)
