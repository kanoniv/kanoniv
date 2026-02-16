"""
Identity Resolution with Splink (Open-Source)
==============================================

Solves the same 5-source identity resolution problem as the before/ (manual SQL)
and after/ (Kanoniv) approaches, using Splink  - the most popular open-source
probabilistic record linkage library.

Splink implements Fellegi-Sunter statistical matching with EM training,
blocking, Jaro-Winkler comparisons, and graph-based clustering.

Backend: DuckDB (in-process, no infrastructure needed).

Usage:
    pip install -r requirements.txt
    python resolve.py
"""

import os
import re
import time
from collections import defaultdict

import pandas as pd
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SEED_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

# Source priority for golden record assembly (highest -> lowest trust)
SOURCE_PRIORITY = ["crm", "billing", "app", "support", "partners"]

# Common nickname -> canonical mappings
NICKNAMES = {
    "bob": "robert", "rob": "robert", "robbie": "robert",
    "bill": "william", "will": "william", "willy": "william",
    "dick": "richard", "rick": "richard", "rich": "richard",
    "jim": "james", "jimmy": "james", "jamie": "james",
    "mike": "michael", "mikey": "michael",
    "jen": "jennifer", "jenny": "jennifer",
    "liz": "elizabeth", "beth": "elizabeth", "betty": "elizabeth",
    "pat": "patricia", "patty": "patricia",
    "chris": "christopher",
    "kate": "katherine", "kathy": "katherine", "katie": "katherine",
    "ben": "benjamin",
    "nick": "nicholas",
    "tom": "thomas", "tommy": "thomas",
    "dan": "daniel", "danny": "daniel",
    "dave": "david",
    "steve": "steven", "stephen": "steven",
    "joe": "joseph", "joey": "joseph",
    "tony": "anthony",
    "ed": "edward", "eddie": "edward",
    "sam": "samuel", "sammy": "samuel",
    "matt": "matthew",
    "andy": "andrew", "drew": "andrew",
    "alex": "alexander",
    "charlie": "charles", "chuck": "charles",
    "harry": "henry",
    "jack": "john", "johnny": "john",
    "larry": "lawrence",
    "jerry": "gerald",
    "terry": "terrence",
    "ray": "raymond",
    "al": "alan",
}

# Domain alias normalization
DOMAIN_ALIASES = {
    "googlemail.com": "gmail.com",
    "hotmail.co.uk": "hotmail.com",
    "live.com": "outlook.com",
}

# Company suffix patterns to strip
COMPANY_SUFFIXES = re.compile(
    r"\s+(inc\.?|incorporated|corp\.?|corporation|llc|l\.l\.c\."
    r"|ltd\.?|limited|co\.?|company)$",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------


def normalize_name(name: str | None) -> str | None:
    """Title-case, resolve nicknames, strip leading/trailing junk."""
    if not name or not isinstance(name, str):
        return None
    name = name.strip()
    if not name:
        return None
    name = name.strip().title()
    lower = name.lower()
    if lower in NICKNAMES:
        name = NICKNAMES[lower].title()
    return name


def normalize_email(email: str | None) -> str | None:
    """Lowercase, strip plus-addressing, Gmail dot trick, domain aliases."""
    if not email or not isinstance(email, str):
        return None
    email = email.strip().lower()
    if "@" not in email:
        return None
    local, domain = email.rsplit("@", 1)
    if "+" in local:
        local = local.split("+")[0]
    domain = DOMAIN_ALIASES.get(domain, domain)
    if domain in ("gmail.com",):
        local = local.replace(".", "")
    return f"{local}@{domain}"


def normalize_phone(phone: str | None) -> str | None:
    """Strip to digits, normalize to +1XXXXXXXXXX (US E.164)."""
    if not phone or not isinstance(phone, str):
        return None
    digits = re.sub(r"[^0-9]", "", phone)
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    elif len(digits) == 10:
        return f"+1{digits}"
    elif len(digits) >= 10:
        return f"+{digits}"
    return None


def normalize_company(company: str | None) -> str | None:
    """Uppercase, strip suffixes."""
    if not company or not isinstance(company, str):
        return None
    company = company.strip().upper()
    company = COMPANY_SUFFIXES.sub("", company).strip()
    if not company:
        return None
    return company


def split_display_name(display_name: str | None) -> tuple[str | None, str | None]:
    """Split 'First Last' into (first, last)."""
    if not display_name or not isinstance(display_name, str):
        return None, None
    parts = display_name.strip().split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    elif len(parts) == 1:
        return None, parts[0]
    return None, None


def parse_billing_name(account_name: str | None) -> tuple[str | None, str | None]:
    """Parse 'Last, First' or 'First Last' billing name format."""
    if not account_name or not isinstance(account_name, str):
        return None, None
    name = account_name.strip()
    if "," in name:
        parts = name.split(",", 1)
        return parts[1].strip(), parts[0].strip()
    parts = name.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return None, name


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_and_normalize() -> pd.DataFrame:
    """Load all 5 source CSVs, normalize, and union into a single spine."""

    records = []

    crm = pd.read_csv(os.path.join(SEED_DIR, "crm_contacts.csv"))
    for _, row in crm.iterrows():
        records.append({
            "source_id": row["crm_contact_id"],
            "source_system": "crm",
            "first_name": normalize_name(row.get("first_name")),
            "last_name": normalize_name(row.get("last_name")),
            "email": normalize_email(row.get("email")),
            "phone": normalize_phone(row.get("phone")),
            "company": normalize_company(row.get("company_name")),
        })

    billing = pd.read_csv(os.path.join(SEED_DIR, "billing_accounts.csv"))
    for _, row in billing.iterrows():
        first, last = parse_billing_name(row.get("account_name"))
        records.append({
            "source_id": row["billing_account_id"],
            "source_system": "billing",
            "first_name": normalize_name(first),
            "last_name": normalize_name(last),
            "email": normalize_email(row.get("email")),
            "phone": None,
            "company": normalize_company(row.get("company_name")),
        })

    support = pd.read_csv(os.path.join(SEED_DIR, "support_users.csv"))
    for _, row in support.iterrows():
        first, last = split_display_name(row.get("display_name"))
        records.append({
            "source_id": row["support_user_id"],
            "source_system": "support",
            "first_name": normalize_name(first),
            "last_name": normalize_name(last),
            "email": normalize_email(row.get("email")),
            "phone": normalize_phone(row.get("phone")),
            "company": normalize_company(row.get("company")),
        })

    app = pd.read_csv(os.path.join(SEED_DIR, "app_signups.csv"))
    for _, row in app.iterrows():
        records.append({
            "source_id": row["app_user_id"],
            "source_system": "app",
            "first_name": normalize_name(row.get("first_name")),
            "last_name": normalize_name(row.get("last_name")),
            "email": normalize_email(row.get("email")),
            "phone": None,
            "company": None,
        })

    partners = pd.read_csv(os.path.join(SEED_DIR, "partner_leads.csv"))
    for _, row in partners.iterrows():
        records.append({
            "source_id": row["partner_lead_id"],
            "source_system": "partners",
            "first_name": normalize_name(row.get("first_name")),
            "last_name": normalize_name(row.get("last_name")),
            "email": normalize_email(row.get("email")),
            "phone": None,
            "company": normalize_company(row.get("company")),
        })

    df = pd.DataFrame(records)
    df["unique_id"] = range(len(df))

    print(f"Loaded {len(df):,} records from 5 sources:")
    for src, count in df["source_system"].value_counts().items():
        nulls = df[df["source_system"] == src][["first_name", "email", "phone"]].isna().sum()
        print(f"  {src:>10}: {count:>5} records  "
              f"(missing: name={nulls['first_name']}, email={nulls['email']}, phone={nulls['phone']})")

    return df


# ---------------------------------------------------------------------------
# Splink model
# ---------------------------------------------------------------------------


def build_settings() -> SettingsCreator:
    """Define the Splink comparison model and blocking rules."""

    return SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="unique_id",

        comparisons=[
            cl.ExactMatch("email"),
            cl.ExactMatch("phone"),
            cl.JaroWinklerAtThresholds("first_name", [0.92, 0.80]),
            cl.JaroWinklerAtThresholds("last_name", [0.92, 0.80]),
            cl.JaroWinklerAtThresholds("company", [0.88]),
        ],

        blocking_rules_to_generate_predictions=[
            block_on("email"),
            block_on("phone"),
            block_on("last_name", "first_name"),
            block_on("company", "last_name"),
        ],
    )


def train_model(linker: Linker):
    """Train the Fellegi-Sunter model via random sampling + EM."""

    print("\n-- Training Fellegi-Sunter model --")

    # Step 1: Estimate u-probabilities from random sampling (no blocking needed)
    linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000)

    # Step 2: Estimate probability two random records match
    linker.training.estimate_probability_two_random_records_match(
        [block_on("email")],
        recall=0.8,
    )

    # Step 3: EM passes for m-probabilities only (u already estimated)
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("email"),
        fix_u_probabilities=True,
    )

    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("last_name", "first_name"),
        fix_u_probabilities=True,
    )

    print("Training complete.")


# ---------------------------------------------------------------------------
# Golden record assembly
# ---------------------------------------------------------------------------


def build_golden_records(df: pd.DataFrame, clusters: pd.DataFrame) -> pd.DataFrame:
    """For each cluster, pick the best value per field using source priority."""

    merged = df.merge(
        clusters[["unique_id", "cluster_id"]],
        on="unique_id",
        how="inner",
    )

    priority_map = {src: i for i, src in enumerate(SOURCE_PRIORITY)}
    merged["source_rank"] = merged["source_system"].map(priority_map)
    merged = merged.sort_values(["cluster_id", "source_rank"])

    golden = []
    for cluster_id, group in merged.groupby("cluster_id"):
        record = {"entity_id": cluster_id}

        for field in ["first_name", "last_name", "email", "phone", "company"]:
            non_null = group[group[field].notna()][field]
            record[field] = non_null.iloc[0] if len(non_null) > 0 else None

        record["num_sources"] = group["source_system"].nunique()
        record["num_source_records"] = len(group)
        record["sources_present"] = sorted(group["source_system"].unique().tolist())

        golden.append(record)

    return pd.DataFrame(golden)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    t0 = time.time()

    # 1. Load & normalize
    df = load_and_normalize()

    # 2. Initialize Splink
    db_api = DuckDBAPI()
    settings = build_settings()
    linker = Linker(df, settings, db_api)

    # 3. Train model
    train_model(linker)

    # 4. Predict pairwise match probabilities
    print("\n-- Predicting pairwise matches --")
    predictions = linker.inference.predict(threshold_match_probability=0.5)
    pred_df = predictions.as_pandas_dataframe()
    print(f"Found {len(pred_df):,} candidate pairs above threshold")

    if len(pred_df) > 0:
        print(f"  Match probability: min={pred_df['match_probability'].min():.3f}, "
              f"median={pred_df['match_probability'].median():.3f}, "
              f"max={pred_df['match_probability'].max():.3f}")

    # 5. Cluster into entities
    print("\n-- Clustering entities --")
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions,
        threshold_match_probability=0.5,
    )
    cluster_df = clusters.as_pandas_dataframe()
    n_entities = cluster_df["cluster_id"].nunique()
    print(f"Resolved {len(df):,} records -> {n_entities:,} entities")

    # 6. Build golden records
    print("\n-- Building golden records --")
    golden = build_golden_records(df, cluster_df)

    # 7. Save outputs
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    golden.to_csv(os.path.join(OUTPUT_DIR, "resolved_entities.csv"), index=False)
    cluster_df.to_csv(os.path.join(OUTPUT_DIR, "entity_graph.csv"), index=False)

    elapsed = time.time() - t0

    # 8. Print summary
    print(f"\n{'='*60}")
    print(f"RESULTS SUMMARY")
    print(f"{'='*60}")
    print(f"Input records:        {len(df):>8,}")
    print(f"Resolved entities:    {n_entities:>8,}")
    print(f"Compression ratio:    {len(df)/n_entities:>8.1f}x")
    print(f"Candidate pairs:      {len(pred_df):>8,}")
    print(f"Elapsed time:         {elapsed:>7.1f}s")
    print()

    sizes = golden["num_source_records"]
    print("Entity size distribution:")
    print(f"  Singletons (1 record):   {(sizes == 1).sum():,}")
    print(f"  2-3 records:             {((sizes >= 2) & (sizes <= 3)).sum():,}")
    print(f"  4-5 records:             {((sizes >= 4) & (sizes <= 5)).sum():,}")
    print(f"  6+ records:              {(sizes >= 6).sum():,}")
    print(f"  Largest cluster:         {sizes.max()} records")
    print()

    print("Source coverage in resolved entities:")
    for src in SOURCE_PRIORITY:
        count = golden["sources_present"].apply(lambda x: src in x).sum()
        print(f"  {src:>10}: {count:>5} entities ({count/n_entities*100:.0f}%)")
    print()

    print("Golden record field completeness:")
    for field in ["first_name", "last_name", "email", "phone", "company"]:
        filled = golden[field].notna().sum()
        print(f"  {field:>12}: {filled:>5}/{n_entities} ({filled/n_entities*100:.0f}%)")
    print()

    print(f"Output saved to {OUTPUT_DIR}/")
    print(f"  resolved_entities.csv  - {len(golden):,} golden records")
    print(f"  entity_graph.csv       - {len(cluster_df):,} source -> entity mappings")


if __name__ == "__main__":
    main()
