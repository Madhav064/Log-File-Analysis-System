from app.services.stages import BY_SLUG, STAGES, neighbours, schema

from loganalysis.contracts import REPORTS, TABLES


def test_ten_stages_numbered_in_order_with_unique_slugs():
    assert [s["number"] for s in STAGES] == [f"{i:02d}" for i in range(1, 11)]
    assert len(BY_SLUG) == 10


def test_every_table_a_stage_names_exists_in_the_contracts():
    for s in STAGES:
        for name in s["tables"]:
            assert name in TABLES or name in REPORTS, (s["slug"], name)
            assert schema(name)


def test_every_analytics_table_is_shown_on_some_page():
    shown = {name for s in STAGES for name in s["tables"]}
    assert set(TABLES) <= shown


def test_paths_come_from_the_cluster_config():
    assert BY_SLUG["etl"]["input"].startswith("hdfs://namenode:9000/logs/raw")


def test_previous_and_next():
    assert neighbours("overview") == (None, BY_SLUG["architecture"])
    assert neighbours("export") == (BY_SLUG["engines"], None)
