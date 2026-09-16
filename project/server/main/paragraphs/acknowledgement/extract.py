FIELDS_MAPPING = {
    "funding-extraction-llama-31-8b-instruct": [{"field": "funder_name", "parent_field": "projects", "parent_type": list}],
    "baguette-funders-600m-4k-with-template": [
        {"field": "funders"},
        {"field": "infrastructures"},
        {"field": "private_companies"},
    ],
}


def extract_results(data: dict, model_name: str) -> int:
    """Count extracted results from LLM output."""

    if not data:
        return 0

    total = 0

    for field_info in FIELDS_MAPPING[model_name]:
        field = field_info["field"]
        parent_field = field_info.get("parent_field")

        root = data

        if parent_field:
            root = root.get(parent_field)

            if not root:
                continue

        if isinstance(root, list):
            total += sum(1 for item in root if item.get(field))
        else:
            if isinstance(root.get(field), list):
                total += len(root[field])
            else:
                total += int(bool(root.get(field)))

    return total
