from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from faers_radar import transforms

DEMO_COLS = [
    "primaryid",
    "caseid",
    "fda_dt",
    "sex",
    "age",
    "age_cod",
    "occr_country",
    "source_quarter",
]


def test_latest_case_versions_keeps_newest_and_drops_deleted(spark):
    demo = spark.createDataFrame(
        [
            ("11", "1", "20250101", "F", "60", "YR", "US", "2025Q1"),
            ("12", "1", "20250401", "F", "61", "YR", "US", "2025Q2"),  # newer version
            ("21", "2", "20250101", "M", "40", "YR", "US", "2025Q1"),
            ("22", "2", "20250101", "M", "40", "YR", "US", "2025Q1"),  # same date, higher id
            ("31", "3", "20250101", "M", "30", "YR", "US", "2025Q1"),  # deleted
        ],
        DEMO_COLS,
    )
    deleted = spark.createDataFrame([("3",)], ["caseid"])

    result = transforms.latest_case_versions(demo, deleted).select("caseid", "primaryid", "period")

    assertDataFrameEqual(
        result,
        spark.createDataFrame([("1", "12", "2025Q2"), ("2", "22", "2025Q1")], result.columns),
    )


def test_normalize_drug_prefers_active_ingredient_and_strips_doses(spark):
    df = spark.createDataFrame(
        [
            ("OZEMPIC", "SEMAGLUTIDE"),
            ("Ozempic 1mg pen", None),
            ("Tylenol 500 MG.", ""),
            ("NORCO", "ACETAMINOPHEN\\HYDROCODONE BITARTRATE"),
            ("   ", None),
        ],
        ["drugname", "prod_ai"],
    )

    result = df.select(transforms.normalize_drug(F.col("drugname"), F.col("prod_ai")).alias("drug"))

    assertDataFrameEqual(
        result,
        spark.createDataFrame(
            [
                ("SEMAGLUTIDE",),
                ("OZEMPIC PEN",),
                ("TYLENOL",),
                ("ACETAMINOPHEN\\HYDROCODONE BITARTRATE",),
                (None,),
            ],
            "drug string",
        ),
    )


def test_disproportionality_matches_hand_computed_2x2(spark):
    # 6 cases. Drug X in 1,2,3; reaction R in 1,2,4.
    # For (X, R): a=2, b=1, c=1, d=2 -> PRR = (2/3)/(1/3) = 2, ROR = (2*2)/(1*1) = 4.
    drugs = spark.createDataFrame(
        [
            ("Q", c, d)
            for c, d in [("1", "X"), ("2", "X"), ("3", "X"), ("4", "Y"), ("5", "Y"), ("6", "Y")]
        ],
        ["period", "caseid", "drug"],
    )
    reactions = spark.createDataFrame(
        [
            ("Q", c, p)
            for c, p in [("1", "R"), ("2", "R"), ("3", "S"), ("4", "R"), ("5", "S"), ("6", "S")]
        ],
        ["period", "caseid", "pt"],
    )

    result = transforms.disproportionality(drugs, reactions, min_cases=1).filter(
        "drug = 'X' AND pt = 'R'"
    )

    assertDataFrameEqual(
        result.select("cases", "b", "c", "d", "prr", "ror"),
        spark.createDataFrame(
            [(2, 1, 1, 2, 2.0, 4.0)], "cases long, b long, c long, d long, prr double, ror double"
        ),
    )


def test_disproportionality_filters_rare_pairs_and_survives_zero_cells(spark):
    # Every X case has R, so b = 0: ROR divides by zero and must come back null, not raise.
    drugs = spark.createDataFrame([("Q", "1", "X"), ("Q", "2", "Y")], ["period", "caseid", "drug"])
    reactions = spark.createDataFrame(
        [("Q", "1", "R"), ("Q", "2", "S")], ["period", "caseid", "pt"]
    )

    assert transforms.disproportionality(drugs, reactions, min_cases=2).count() == 0

    row = transforms.disproportionality(drugs, reactions, min_cases=1).filter("drug = 'X'").first()
    assert row.ror is None
    assert row.is_signal is not True
