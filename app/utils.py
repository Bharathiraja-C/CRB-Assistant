import re

def split_sql_ctes(sql_text: str):
    """
    Split a SQL script into CTE blocks.
    Returns list of (cte_name, cte_sql).
    """
    # Match WITH or , CTE_NAME AS (
    pattern = re.compile(r"(?i)(?:(?:with)|(?:,))\s+(\w+)\s+as\s*\(", re.IGNORECASE)
    matches = list(pattern.finditer(sql_text))

    blocks = []
    for i, match in enumerate(matches):
        cte_name = match.group(1)
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(sql_text)
        cte_sql = sql_text[start:end].strip().rstrip(",")  # remove trailing comma
        blocks.append((cte_name, cte_sql))

    return blocks
