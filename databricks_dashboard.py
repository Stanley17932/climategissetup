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

Two conventions here are not cosmetic, they are what makes tiles render:

* Every dataset query line is given a trailing newline except the last. The API
  joins `queryLines` without inserting whitespace, so anything else welds tokens
  together (`SELECTCOUNT(*)`).
* Tables use spec version 1 with fully described columns. Version 3 with bare
  `{fieldName}` entries publishes without error and then renders nothing.

One caveat is baked into the labels: the current year is pulled from the
Climate TRACE API as it stands, which is year-to-date. In the 2026 snapshot its
activity is about two-thirds of 2025, so it is shown as YTD and never as a
like-for-like drop. Counters and rankings read the last complete year.
"""

import json
import sys

import databricks_push as push

DASHBOARD_NAME = "Climate TRACE - Facility Emissions"
LAST_COMPLETE = "(SELECT MAX(year) FROM {table}) - 1"


def sql_lines(text):
    """The API joins queryLines without whitespace; each line keeps its own."""
    lines = text.strip("\n").split("\n")
    return [line if i == len(lines) - 1 else line + "\n" for i, line in enumerate(lines)]


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
            "sector_share",
            "sector share, last complete year",
            f"""
SELECT sector AS `Sector`,
       CAST(ROUND(SUM(emissions_quantity) / 1e6, 0) AS DECIMAL(18,0)) AS `Mt CO2`
FROM {table}
WHERE year = {last}
GROUP BY sector
ORDER BY `Mt CO2` DESC
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
            "intensity",
            "power intensity, last complete year",
            f"""
SELECT country AS `Country`,
       CAST(ROUND(SUM(emissions_quantity) * 1000.0 / NULLIF(SUM(activity), 0), 0) AS DECIMAL(10,0)) AS `kg CO2 per MWh`,
       CAST(ROUND(SUM(emissions_quantity) / 1e6, 0) AS DECIMAL(18,0)) AS `Mt CO2`
FROM {table}
WHERE year = {last} AND sector = 'power' AND activity > 0
GROUP BY country
ORDER BY `Mt CO2` DESC
LIMIT 10
""",
        ),
        dataset(
            "trend",
            "emissions by year and sector",
            f"""
SELECT TO_DATE(CONCAT(year, '-01-01')) AS `Year`,
       sector AS `Sector`,
       CAST(ROUND(SUM(emissions_quantity) / 1e6, 0) AS DECIMAL(18,0)) AS `Mt CO2`
FROM {table}
GROUP BY 1, 2
ORDER BY 1, 2
""",
        ),
        dataset(
            "power_scatter",
            "largest power assets, last complete year",
            f"""
SELECT name AS `Facility`,
       country AS `Country`,
       ROUND(capacity, 0) AS `Capacity (MW)`,
       ROUND(emissions_quantity / 1e6, 2) AS `Mt CO2`,
       CASE WHEN country IN ('CHN', 'USA', 'IND', 'RUS', 'JPN') THEN country ELSE 'Other' END AS `Group`
FROM {table}
WHERE year = {last} AND sector = 'power' AND capacity > 0 AND emissions_quantity > 0
ORDER BY emissions_quantity DESC
LIMIT 500
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


def counter(name, title, field, x, y, width, height, places=0):
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": "summary",
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
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def chart(name, title, kind, dataset_name, encodings, fields, x, y, width, height, disagg=True):
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [{"name": f, "expression": f"`{f}`"} for f in fields],
                        "disaggregated": disagg,
                    },
                }
            ],
            "spec": {
                "version": 3,
                "frame": {"title": title, "showTitle": True, "showDescription": False},
                "widgetType": kind,
                "encodings": encodings,
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def column(field, kind="string", order=0, number_format=None):
    return {
        "fieldName": field,
        "displayName": field,
        "title": field,
        "type": kind,
        "displayAs": "number" if kind in ("number", "integer") else kind,
        "visible": True,
        "order": order,
        "allowSearch": kind == "string",
        "alignContent": "right" if kind in ("number", "integer") else "left",
        "linkUrlTemplate": "{{ @ }}",
        "linkTextTemplate": "{{ @ }}",
        "linkTitleTemplate": "{{ @ }}",
        "linkOpenInNewTab": True,
        "highlightLinks": False,
        "allowHTML": False,
        "useMonospaceFont": False,
        "preserveWhitespace": False,
        "imageUrlTemplate": "{{ @ }}",
        "imageTitleTemplate": "{{ @ }}",
        "imageWidth": "",
        "imageHeight": "",
        "booleanValues": ["false", "true"],
        **({"numberFormat": number_format} if number_format else {}),
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
                "version": 1,
                "widgetType": "table",
                "allowHTMLByDefault": False,
                "condensed": True,
                "withRowNumber": False,
                "itemsPerPage": 15,
                "paginationSize": "default",
                "invisibleColumns": [],
                "frame": {"title": title, "showTitle": True, "showDescription": False},
                "encodings": {
                    "columns": [
                        column("Facility"),
                        column("Country"),
                        column("Sector"),
                        column("Source"),
                        column("Mt CO2", kind="number", order=4, number_format="0.00"),
                    ]
                },
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def page():
    return {
        "name": "page_overview",
        "displayName": "Overview",
        "pageType": "PAGE_TYPE_CANVAS",
        "layoutVersion": "GRID_V1",
        "layout": [
            textbox(
                "title",
                "# Climate TRACE - facility emissions\n"
                "Facilities, countries and sectors as the pipeline last loaded them.",
                0, 0, 12, 2,
            ),
            counter("c_assets", "Assets tracked", "Assets", 0, 2, 3, 2),
            counter("c_countries", "Countries", "Countries", 3, 2, 3, 2),
            counter("c_sectors", "Sectors", "Sectors", 6, 2, 3, 2),
            counter("c_gt", "Gt CO2 (last complete year)", "Gt CO2 (last complete year)", 9, 2, 3, 2, places=2),
            chart(
                "b_sector", "Emissions by sector - current year is YTD", "bar", "sector_year",
                {
                    "x": {"fieldName": "Sector", "scale": {"type": "categorical"}, "displayName": "Sector"},
                    "y": {"fieldName": "Mt CO2", "scale": {"type": "quantitative"}, "displayName": "Mt CO2"},
                    "color": {"fieldName": "Year", "scale": {"type": "categorical"}, "displayName": "Year"},
                },
                ["Sector", "Year", "Mt CO2"], 0, 4, 6, 7,
            ),
            chart(
                "d_sector", "Share of emissions - last complete year", "donut", "sector_share",
                {
                    "color": {"fieldName": "Sector", "scale": {"type": "categorical"}, "displayName": "Sector"},
                    "angle": {"fieldName": "Mt CO2", "scale": {"type": "quantitative"}, "displayName": "Mt CO2"},
                },
                ["Sector", "Mt CO2"], 6, 4, 6, 7,
            ),
            chart(
                "b_countries", "Top 10 countries - last complete year", "bar", "top_countries",
                {
                    "x": {"fieldName": "Mt CO2", "scale": {"type": "quantitative"}, "displayName": "Mt CO2"},
                    "y": {"fieldName": "Country", "scale": {"type": "categorical"}, "displayName": "Country"},
                    "label": {"show": True},
                },
                ["Country", "Mt CO2"], 0, 11, 6, 7,
            ),
            chart(
                "b_intensity", "Power intensity - kg CO2 per MWh", "bar", "intensity",
                {
                    "x": {"fieldName": "kg CO2 per MWh", "scale": {"type": "quantitative"}, "displayName": "kg CO2 per MWh"},
                    "y": {"fieldName": "Country", "scale": {"type": "categorical"}, "displayName": "Country"},
                    "label": {"show": True},
                },
                ["Country", "kg CO2 per MWh", "Mt CO2"], 6, 11, 6, 7,
            ),
            chart(
                "l_trend", "Emissions by year - current year is YTD", "line", "trend",
                {
                    "x": {"fieldName": "Year", "scale": {"type": "temporal"}, "displayName": "Year"},
                    "y": {"fieldName": "Mt CO2", "scale": {"type": "quantitative"}, "displayName": "Mt CO2"},
                    "color": {"fieldName": "Sector", "scale": {"type": "categorical"}, "displayName": "Sector"},
                },
                ["Year", "Sector", "Mt CO2"], 0, 18, 6, 7,
            ),
            chart(
                "s_power", "Largest 500 power assets - capacity vs emissions", "scatter", "power_scatter",
                {
                    "x": {"fieldName": "Capacity (MW)", "scale": {"type": "quantitative"}, "displayName": "Capacity (MW)"},
                    "y": {"fieldName": "Mt CO2", "scale": {"type": "quantitative"}, "displayName": "Mt CO2"},
                    "color": {"fieldName": "Group", "scale": {"type": "categorical"}, "displayName": "Group"},
                },
                ["Facility", "Country", "Capacity (MW)", "Mt CO2", "Group"], 6, 18, 6, 7,
            ),
            table_widget(
                "t_assets", "Top 15 emitting facilities - last complete year", "top_assets",
                ["Facility", "Country", "Sector", "Source", "Mt CO2"], 0, 25, 12, 9,
            ),
            textbox(
                "note",
                "**Reading the current year.** The API returns the current year "
                "year-to-date: activity in the latest snapshot is roughly two-thirds of "
                "the complete year, so treat that series as partial rather than a fall in "
                "emissions. Rankings, shares and the Gt counter use the last complete year.",
                0, 34, 12, 2,
            ),
        ],
    }


def dashboard_json(table):
    return {"datasets": datasets(table), "pages": [page()]}


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
