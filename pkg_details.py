import pandas as pd
from googleads import ad_manager


def clean_line_item_name(name):
    if pd.isnull(name):
        return name
    while len(name) >= 5 and not name[-5:].isdigit():
        name = name[:-1]
    return name.strip()


def get_line_item_ids_by_name(client, pkg_names, batch_size=140):
    line_item_service = client.GetService('LineItemService', version='v202411')
    all_line_item_details = []

    # Clean input package names once
    cleaned_pkg_names = [clean_line_item_name(name) for name in pkg_names if name]

    # Split pkg_names into batches
    def chunked(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i:i + size]

    for batch in chunked(pkg_names, batch_size):
        like_clauses = [f"name LIKE '%{part}%'" for part in batch]
        query = " OR ".join(like_clauses)
        statement = ad_manager.StatementBuilder(version='v202405').Where(query)

        try:
            response = line_item_service.getLineItemsByStatement(statement.ToStatement())
        except Exception as e:
            print(f"Query failed for batch: {batch[:3]} → {e}")
            continue

        results = getattr(response, 'results', [])
        print(f"Batch size {len(batch)} - Matches found: {len(results)}")

        for item in results:
            line_item_name = getattr(item, 'name', 'Unnamed')
            filtered_name = clean_line_item_name(line_item_name)

            # Match only exact cleaned names
            if filtered_name not in cleaned_pkg_names:
                continue

            # Safely extract impressions
            stats = getattr(item, 'stats', None)
            impression = getattr(stats, 'impressionsDelivered', 0) if stats else 0

            # Categorize
            entry = {
                "line_item_name": filtered_name,
                "impression": 0,
                "psbk": 0,
                "NewsPoint": 0
            }

            name_lower = line_item_name.lower()
            name_upper = line_item_name.upper()

            if any(kw in name_lower for kw in ("passback", "opt", "back", "pb", "ps")):
                entry["psbk"] = impression
            elif any(kw in name_upper for kw in ("NEWSPOINT", "NP", "POINT")):
                entry["NewsPoint"] = impression
            else:
                entry["impression"] = impression

            all_line_item_details.append(entry)

    print(f"Total collected line items: {len(all_line_item_details)}")
    return all_line_item_details
