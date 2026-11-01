"""Create (or refresh) the AI/BI dashboard over the pushed snapshot.

The table is a full snapshot per run, so every tile reads the same table and
nothing needs to be reconciled: the dashboard is just SQL wedges over
`workspace.climatetrace.sources`.

    python databricks_dashboard.py                 # create or update, then publish
    python databricks_dashboard.py --no-publish    # leave it in draft

Environment:
    DATABRICKS_HOST   https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
    DATABRICKS_TOKEN  PAT scoped to files, unity-catalog and sql
    DATABRICKS_TABLE  catalog.schema.table    (default workspace.climatetrace.sources)

One caveat is baked into the labels: the current year is pulled from the
Climate TRACE API as it stands, which is year-to-date. In the 2026 snapshot its
activity is about two-thirds of 2025, so it is shown as YTD and never as a
like-for-like drop. The counters and rankings read the last complete year.
"""

import json
import os
import sys

import databricks_push as push

DASHBOARD_NAME = "Climate TRACE - Facility Emissions"
LAST_COMPLETE = "(SELECT MAX(year) FROM {table}) - 1"


def sql_lines(text):
    """The API wants the query as a list of lines, newlines included."""
    stripped = [line for line in text.strip("\n").split("\n")]
    return [line if i == len(stripped) - 1 else line + "\n" for i, line in enumerate(stripped)]


def dataset(name, display, sql):
    return {"name": name, "displayName": display, "queryLines": sql_lines(sql)}


# --- datasets ------------------------------------------------------------


def datasets(table):
    last = LAST_COMPLETE.format(table=table)
    return [
        dataset(
            "summary",
            "summary",
            f"""
SELECT COUNT(DISTINCT id) AS `Assets`,
       COUNT(DISTINCT country) AS `Countries`,
       COUNT(DISTINCT sector) AS `Sectors`,
       CAST(ROUND(SUM(CASE WHEN year = {last} THEN emissions_quantity END) / 1e9, 2) AS DECIMAL(10,2)) AS `Gt CO2 (last complete year)`
FROM {table}
""",
        ),
        dataset(
            "sector_year",
            "emissions by sector and year",
            f"""
SELECT sector AS `Sector`,
       year AS `Year`,
       CAST(ROUND(SUM(emissions_quantity) / 1e6, 0) AS DECIMAL(18,0)) AS `Mt CO2`
FROM {table}
GROUP BY sector, year
ORDER BY sector, year
""",
        ),
        dataset(
            "top_countries",
            "top countries, last complete year",
            f"""
SELECT country AS `Country`,
       CAST(ROUND(SUM(emissions_quantity) / 1e6, 0) AS DECIMAL(18,0)) AS `Mt CO2`
FROM {table}
WHERE year = {last}
GROUP BY country
ORDER BY `Mt CO2` DESC
LIMIT 10
""",
        ),
        dataset(
            "top_assets",
            "top facilities, last complete year",
            f"""
SELECT name AS `Facility`,
       country AS `Country`,
       sector AS `Sector`,
       source_label AS `Source`,
       CAST(ROUND(emissions_quantity / 1e6, 2) AS DECIMAL(10,2)) AS `Mt CO2`
FROM {table}
WHERE year = {last}
ORDER BY emissions_quantity DESC
LIMIT 15
""",
        ),
    ]


# --- widgets -------------------------------------------------------------


def textbox(name, markdown, x, y, width, height):
    return {
        "widget": {"name": name, "multilineTextboxSpec": {"lines": markdown.split("\n")}},
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def counter(name, title, field, dataset_name="summary", places=0):
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [{"name": field, "expression": f"`{field}`"}],
                        "disaggregated": True,
                    },
                }
            ],
            "spec": {
                "version": 2,
                "frame": {"title": title, "showTitle": True},
                "widgetType": "counter",
                "encodings": {
                    "value": {
                        "fieldName": field,
                        "displayName": title,
                        "format": {
                            "type": "number-plain",
                            "decimalPlaces": {"type": "exact", "places": places},
                            "abbreviation": "none",
                        },
                    }
                },
                "data": {"queryName": "main_query"},
            },
        },
        "position": None,
    }


def bar(name, title, dataset_name, fields, x_field, y_field, color_field=None):
    encodings = {
        "x": {"fieldName": x_field, "scale": {"type": "categorical"}, "displayName": x_field},
        "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_field},
    }
    if color_field:
        encodings["color"] = {
            "fieldName": color_field,
            "scale": {"type": "categorical"},
            "displayName": color_field,
        }
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [{"name": f, "expression": f"`{f}`"} for f in fields],
                        "disaggregated": True,
                    },
                }
            ],
            "spec": {
                "version": 3,
                "frame": {"title": title, "showTitle": True, "showDescription": False},
                "widgetType": "bar",
                "encodings": encodings,
            },
        },
        "position": None,
    }


def table_widget(name, title, dataset_name, columns, x, y, width, height):
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [{"name": c, "expression": f"`{c}`"} for c in columns],
                        "disaggregated": True,
                    },
                }
            ],
            "spec": {
                "version": 3,
                "frame": {"title": title, "showTitle": True, "showDescription": False},
                "widgetType": "table",
                "encodings": {"columns": [{"fieldName": c, "displayName": c} for c in columns]},
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def place(item, x, y, width, height):
    item["position"] = {"x": x, "y": y, "width": width, "height": height}
    return item


def page():
    layout = [
        textbox(
            "title",
            "# Climate TRACE - facility emissions\n"
            "Facilities, countries and sectors as the pipeline last loaded them.",
            0, 0, 12, 2,
        ),
        place(counter("c_assets", "Assets tracked", "Assets"), 0, 2, 3, 2),
        place(counter("c_countries", "Countries", "Countries"), 3, 2, 3, 2),
        place(counter("c_sectors", "Sectors", "Sectors"), 6, 2, 3, 2),
        place(
            counter("c_gt", "Gt CO2 (last complete year)", "Gt CO2 (last complete year)", places=2),
            9, 2, 3, 2,
        ),
        place(
            bar(
                "b_sector",
                "Emissions by sector - current year is YTD",
                "sector_year",
                ["Sector", "Year", "Mt CO2"],
                "Sector",
                "Mt CO2",
                color_field="Year",
            ),
            0, 4, 6, 6,
        ),
        place(
            bar(
                "b_countries",
                "Top 10 countries - last complete year",
                "top_countries",
                ["Country", "Mt CO2"],
                "Country",
                "Mt CO2",
            ),
            6, 4, 6, 6,
        ),
        table_widget(
            "t_assets",
            "Top 15 emitting facilities - last complete year",
            "top_assets",
            ["Facility", "Country", "Sector", "Source", "Mt CO2"],
            0, 10, 12, 8,
        ),
        textbox(
            "note",
            "**Reading the current year.** The API returns the current year "
            "year-to-date: activity in the latest snapshot is roughly two-thirds of "
            "the complete year, so treat that series as partial rather than a fall in "
            "emissions. Rankings and the Gt counter use the last complete year.",
            0, 18, 12, 2,
        ),
    ]
    return {
        "name": "page_overview",
        "displayName": "Overview",
        "pageType": "PAGE_TYPE_CANVAS",
        "layoutVersion": "GRID_V1",
        "layout": layout,
    }


def dashboard_json(table):
    return {
        "datasets": datasets(table),
        "pages": [page()],
    }


# --- wiring --------------------------------------------------------------


def main(argv):
    publish = "--no-publish" not in argv[1:]
    host = push.env("DATABRICKS_HOST")
    token = push.env("DATABRICKS_TOKEN")
    if not host or not token:
        raise SystemExit("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

    table = push.env("DATABRICKS_TABLE", push.DEFAULT_TABLE)
    api = push.Databricks(host, token)
    warehouse = api.warehouse_id()
    serialized = json.dumps(dashboard_json(table), indent=1)
    print(f"workspace {api.host} | table {table} | warehouse {warehouse}")

    existing = [
        d
        for d in api.call("GET", "/api/2.0/lakeview/dashboards").get("dashboards", [])
        if d.get("display_name") == DASHBOARD_NAME
    ]
    body = {
        "display_name": DASHBOARD_NAME,
        "warehouse_id": warehouse,
        "serialized_dashboard": serialized,
    }
    if existing:
        dashboard_id = existing[0]["dashboard_id"]
        api.call("PATCH", f"/api/2.0/lakeview/dashboards/{dashboard_id}", body=body)
        print(f"  updated dashboard {dashboard_id}")
    else:
        dashboard_id = api.call("POST", "/api/2.0/lakeview/dashboards", body=body)["dashboard_id"]
        print(f"  created dashboard {dashboard_id}")

    if publish:
        api.call(
            "POST",
            f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
            body={"warehouse_id": warehouse, "embed_credentials": False},
        )
        print("  published")

    print(f"  {api.host}/dashboardsv3/{dashboard_id}/published")


if __name__ == "__main__":
    main(sys.argv)
