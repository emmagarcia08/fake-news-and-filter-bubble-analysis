from ast import literal_eval
import json
import re

senticnet_dict = {}
input_path = "senticnet.py"
output_path = "senticnet.json"

with open(input_path, "r", encoding="utf-8") as f:
    for line_number, line in enumerate(f, 1):
        match = re.match(r"senticnet\[(.+?)\]\s*=\s*(.+)", line)  # Only lines such as senticnet['...'] = [...] are processed
        if match:
            raw_key = match.group(1).strip()
            raw_value = match.group(2).strip()

            try:
                key = literal_eval(raw_key)
                value = literal_eval(raw_value)
                senticnet_dict[key] = value
            except Exception as e:
                print(f"Line {line_number} ignored (parsing error): {e}")
                continue

with open(output_path, "w", encoding="utf-8") as out:
    json.dump(senticnet_dict, out, ensure_ascii=False, indent=2)

print(f"\n{len(senticnet_dict)} valid entries extracted in '{output_path}'")
