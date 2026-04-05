#!/usr/bin/env python3
import re

content = open(
    "/home/ubuntu/aiostreams/bitmagnet-media/classifier/shared_adult_title_classifier.py"
).read()


def extract_set(name, content):
    pattern = rf"{name} = \{{([^}}]+)\}}"
    match = re.search(pattern, content, re.DOTALL)
    items = []
    for line in match.group(1).split("\n"):
        line = line.strip()
        # Remove leading and trailing quotes
        if line.startswith('"') and line.endswith('",'):
            line = line[1:-2]
        elif line.startswith('"') and line.endswith(","):
            line = line[1:-1]
        elif line.startswith("'") and line.endswith("',"):
            line = line[1:-2]
        elif line.startswith("'") and line.endswith(","):
            line = line[1:-1]
        if line and not line.startswith("#"):
            items.append(line.lower())
    return sorted(set(items))


brands = extract_set("ADULT_BRANDS", content)
performers = extract_set("ADULT_PERFORMERS", content)
scenes = extract_set("ADULT_SCENE_TITLES", content)
cjk = extract_set("ADULT_CJK_TERMS", content)
codes = extract_set("ADULT_CODE_PREFIXES", content)

lines = []
lines.append("// Code generated from Python classifier patterns. DO NOT EDIT.")
lines.append("package dhtcrawler")
lines.append("")
lines.append("// adultBrands contains adult brand/production company names")
lines.append("var adultBrands = []string{")
for b in brands:
    lines.append('\t"' + b + '",')
lines.append("}")
lines.append("")
lines.append("// adultPerformers contains adult performer names")
lines.append("var adultPerformers = []string{")
for p in performers:
    lines.append('\t"' + p + '",')
lines.append("}")
lines.append("")
lines.append("// adultSceneTitles contains adult scene/pornography title patterns")
lines.append("var adultSceneTitles = []string{")
for s in scenes:
    lines.append('\t"' + s + '",')
lines.append("}")
lines.append("")
lines.append("// cjkAdultTerms contains Chinese/Japanese/Korean adult terms")
lines.append("var cjkAdultTerms = []string{")
for c in cjk:
    lines.append('\t"' + c + '",')
lines.append("}")
lines.append("")
lines.append("// adultCodePrefixes contains JAV/adult release code prefixes")
lines.append("var adultCodePrefixes = []string{")
for c in codes:
    lines.append('\t"' + c + '",')
lines.append("}")

output = "\n".join(lines) + "\n"
open(
    "/home/ubuntu/aiostreams/bitmagnet-media/internal/dhtcrawler/patterns_gen.go",
    "w",
).write(output)
print("Generated patterns_gen.go")
