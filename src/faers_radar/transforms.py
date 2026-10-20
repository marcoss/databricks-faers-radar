"""Pure DataFrame -> DataFrame transforms. No I/O, so every step is unit-testable."""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

SUSPECT_ROLES = ("PS", "SS")  # primary / secondary suspect; excludes concomitant drugs


def latest_case_versions(demo: DataFrame, deleted: DataFrame) -> DataFrame:
    """One row per case: its most recent version, minus cases FDA has deleted.

    FDA's recommended dedup: per caseid keep the latest fda_dt, breaking ties on
    the highest primaryid. fda_dt is yyyyMMdd, so string order is date order.
    """
    latest = Window.partitionBy("caseid").orderBy(
        F.col("fda_dt").desc_nulls_last(),
        F.expr("try_cast(primaryid AS BIGINT)").desc_nulls_last(),
    )
    return (
        demo.withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .drop("_rn")
        .join(deleted, "caseid", "left_anti")
        .select(
            "primaryid",
            "caseid",
            F.col("source_quarter").alias("period"),
            "fda_dt",
            "sex",
            "age",
            "age_cod",
            "occr_country",
        )
    )


def normalize_drug(drugname: Column, prod_ai: Column) -> Column:
    """Canonical drug key: active ingredient when FDA mapped one, else the cleaned free text.

    "OZEMPIC 1MG PEN" -> "OZEMPIC PEN"; combos keep FDA's backslash separator.
    """
    ai = F.trim(prod_ai)
    raw = F.upper(F.when(ai != "", ai).otherwise(drugname))
    s = F.regexp_replace(raw, r"\b\d+(\.\d+)?\s*(MG|MCG|UG|G|ML|IU|UNITS?|%)(?=\W|$)", " ")
    s = F.regexp_replace(s, r"[^A-Z0-9\\]+", " ")
    s = F.trim(F.regexp_replace(s, r"\s+", " "))
    return F.when(s != "", s)


def case_drugs(drug: DataFrame, cases: DataFrame) -> DataFrame:
    """Distinct (period, caseid, drug) for suspect drugs on the kept case version."""
    return (
        drug.filter(F.col("role_cod").isin(*SUSPECT_ROLES))
        .join(cases.select("primaryid", "caseid", "period"), ["primaryid", "caseid"])
        .select(
            "period", "caseid", normalize_drug(F.col("drugname"), F.col("prod_ai")).alias("drug")
        )
        .filter(F.col("drug").isNotNull())
        .distinct()
    )


def case_reactions(reac: DataFrame, cases: DataFrame) -> DataFrame:
    """Distinct (period, caseid, pt) MedDRA preferred terms on the kept case version."""
    return (
        reac.join(cases.select("primaryid", "caseid", "period"), ["primaryid", "caseid"])
        .select("period", "caseid", F.initcap(F.trim("pt")).alias("pt"))
        .filter(F.col("pt") != "")
        .distinct()
    )


def disproportionality(drugs: DataFrame, reactions: DataFrame, min_cases: int = 3) -> DataFrame:
    """PRR, ROR (95% CI) and Yates chi-square per (period, drug, reaction).

    2x2 table per pair, counted in cases:
                    reaction   other
        drug           a         b
        other          c         d
    Signal (Evans 2001): a >= 3, PRR >= 2, chi2 >= 4; also require ROR lower CI > 1.
    """
    # Universe: cases with at least one suspect drug AND one reaction.
    universe = (
        drugs.select("period", "caseid")
        .distinct()
        .join(reactions.select("period", "caseid").distinct(), ["period", "caseid"])
    )
    drugs = drugs.join(universe, ["period", "caseid"], "left_semi")
    reactions = reactions.join(universe, ["period", "caseid"], "left_semi")

    # The big shuffle: every drug x reaction within a case. Polypharmacy cases
    # (dozens of drugs x dozens of reactions) skew this join; AQE splits them.
    pairs = (
        drugs.join(reactions, ["period", "caseid"])
        .groupBy("period", "drug", "pt")
        .agg(F.count("*").alias("a"))
        .filter(F.col("a") >= min_cases)
    )
    n_drug = drugs.groupBy("period", "drug").agg(F.count("*").alias("n_drug"))
    n_pt = reactions.groupBy("period", "pt").agg(F.count("*").alias("n_pt"))
    n_total = universe.groupBy("period").agg(F.count("*").alias("n"))

    a = F.col("a").cast("double")
    b = (F.col("n_drug") - F.col("a")).cast("double")
    c = (F.col("n_pt") - F.col("a")).cast("double")
    d = (F.col("n") - F.col("n_drug") - F.col("n_pt") + F.col("a")).cast("double")
    n = F.col("n").cast("double")
    one = F.lit(1.0)

    # try_divide: Spark 4 runs in ANSI mode, where x / 0 raises instead of returning null.
    prr = F.try_divide(F.try_divide(a, a + b), F.try_divide(c, c + d))
    ror = F.try_divide(a * d, b * c)
    se_log_ror = F.sqrt(
        F.try_divide(one, a) + F.try_divide(one, b) + F.try_divide(one, c) + F.try_divide(one, d)
    )
    chi2 = F.try_divide(
        n * F.pow(F.abs(a * d - b * c) - n / 2, 2),
        (a + b) * (c + d) * (a + c) * (b + d),
    )

    return (
        pairs.join(n_drug, ["period", "drug"])
        .join(n_pt, ["period", "pt"])
        .join(F.broadcast(n_total), "period")
        .select(
            "period",
            "drug",
            "pt",
            F.col("a").alias("cases"),
            b.cast("long").alias("b"),
            c.cast("long").alias("c"),
            d.cast("long").alias("d"),
            prr.alias("prr"),
            ror.alias("ror"),
            F.exp(F.log(ror) - 1.96 * se_log_ror).alias("ror_lower"),
            F.exp(F.log(ror) + 1.96 * se_log_ror).alias("ror_upper"),
            chi2.alias("chi2"),
        )
        .withColumn(
            "is_signal",
            (F.col("cases") >= 3)
            & (F.col("prr") >= 2)
            & (F.col("chi2") >= 4)
            & (F.col("ror_lower") > 1),
        )
    )
