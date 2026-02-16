"""
Standalone DuckDB script that mirrors the dbt-sql identity resolution pipeline.
Useful for quick local testing without dbt installed.

Produces the same results as: dbt seed && dbt run --profiles-dir . --target local
"""
import duckdb
import os

data_path = os.path.join(os.path.dirname(__file__), "..", "data")

con = duckdb.connect()

print("Loading data...")
sources = {
    "crm_contacts": f"{data_path}/crm_contacts.csv",
    "billing_accounts": f"{data_path}/billing_accounts.csv",
    "support_users": f"{data_path}/support_users.csv",
    "app_signups": f"{data_path}/app_signups.csv",
    "partner_leads": f"{data_path}/partner_leads.csv",
}

for name, path in sources.items():
    con.execute(f"CREATE TABLE {name} AS SELECT * FROM read_csv_auto('{path}')")

# 1. Spine + normalization + nickname expansion
con.execute("""
CREATE TABLE int_identity__spine AS
WITH raw_union AS (
    SELECT crm_contact_id as source_id, 'crm' as source_system, first_name, last_name, email, phone, company_name FROM crm_contacts
    UNION ALL
    SELECT billing_account_id, 'billing', trim(split_part(account_name, ',', 2)), trim(split_part(account_name, ',', 1)), email, NULL, company_name FROM billing_accounts
    UNION ALL
    SELECT support_user_id, 'support', split_part(display_name, ' ', 1), split_part(display_name, ' ', 2), email, phone, company FROM support_users
    UNION ALL
    SELECT app_user_id, 'app', first_name, last_name, email, NULL, NULL FROM app_signups
    UNION ALL
    SELECT partner_lead_id, 'partners', first_name, last_name, email, NULL, company FROM partner_leads
),
base_norm AS (
    SELECT
        source_id,
        source_system,
        lower(trim(first_name)) as first_name,
        lower(trim(last_name)) as last_name,
        lower(trim(
            case
                when split_part(email, '@', 2) in ('gmail.com', 'googlemail.com')
                then replace(split_part(split_part(email, '@', 1), '+', 1), '.', '') || '@gmail.com'
                when email like '%+%'
                then split_part(email, '+', 1) || '@' || split_part(email, '@', 2)
                else email
            end
        )) as email,
        regexp_replace(phone, '[^0-9]', '', 'g') as phone,
        lower(trim(company_name)) as company_name,
        md5(source_system || source_id) as record_id
    FROM raw_union
),
nickname_norm AS (
    SELECT
        *,
        CASE first_name
            WHEN 'bob' THEN 'robert'    WHEN 'bobby' THEN 'robert'   WHEN 'rob' THEN 'robert'
            WHEN 'bill' THEN 'william'  WHEN 'billy' THEN 'william'  WHEN 'will' THEN 'william'
            WHEN 'mike' THEN 'michael'  WHEN 'mikey' THEN 'michael'
            WHEN 'jim' THEN 'james'     WHEN 'jimmy' THEN 'james'    WHEN 'jamie' THEN 'james'
            WHEN 'liz' THEN 'elizabeth' WHEN 'beth' THEN 'elizabeth' WHEN 'lizzy' THEN 'elizabeth'
            WHEN 'jen' THEN 'jennifer'  WHEN 'jenny' THEN 'jennifer'
            WHEN 'kate' THEN 'katherine' WHEN 'kathy' THEN 'katherine' WHEN 'katie' THEN 'katherine'
            WHEN 'dick' THEN 'richard'  WHEN 'rick' THEN 'richard'   WHEN 'rich' THEN 'richard'
            WHEN 'tom' THEN 'thomas'    WHEN 'tommy' THEN 'thomas'
            WHEN 'dan' THEN 'daniel'    WHEN 'danny' THEN 'daniel'
            WHEN 'dave' THEN 'david'
            WHEN 'ed' THEN 'edward'     WHEN 'ted' THEN 'edward'
            WHEN 'joe' THEN 'joseph'    WHEN 'joey' THEN 'joseph'
            WHEN 'chris' THEN 'christopher'
            WHEN 'matt' THEN 'matthew'
            WHEN 'pat' THEN 'patrick'
            WHEN 'steve' THEN 'stephen'
            WHEN 'tony' THEN 'anthony'
            WHEN 'nick' THEN 'nicholas'
            WHEN 'alex' THEN 'alexander'
            WHEN 'sam' THEN 'samuel'
            WHEN 'ben' THEN 'benjamin'
            WHEN 'charlie' THEN 'charles' WHEN 'chuck' THEN 'charles'
            WHEN 'sue' THEN 'susan'     WHEN 'suzy' THEN 'susan'
            WHEN 'meg' THEN 'margaret'  WHEN 'maggie' THEN 'margaret' WHEN 'peggy' THEN 'margaret'
            ELSE first_name
        END as norm_first_name
    FROM base_norm
)
SELECT * FROM nickname_norm
""")

# 2. Blocking (5 keys)
con.execute("""
CREATE TABLE int_identity__blocking AS
WITH blocking_keys AS (
    SELECT record_id, email as match_key FROM int_identity__spine WHERE email IS NOT NULL
    UNION ALL
    SELECT record_id, phone FROM int_identity__spine WHERE phone IS NOT NULL AND phone <> ''
    UNION ALL
    SELECT record_id, norm_first_name || '|' || last_name FROM int_identity__spine WHERE norm_first_name IS NOT NULL AND last_name IS NOT NULL
    UNION ALL
    SELECT record_id, split_part(email, '@', 1) || '|' || last_name FROM int_identity__spine WHERE email IS NOT NULL AND last_name IS NOT NULL
    UNION ALL
    SELECT record_id, last_name || '|' || company_name FROM int_identity__spine WHERE last_name IS NOT NULL AND company_name IS NOT NULL AND company_name <> ''
)
SELECT
    a.record_id as record_a,
    b.record_id as record_b,
    count(*) as shared_keys
FROM blocking_keys a
JOIN blocking_keys b ON a.match_key = b.match_key AND a.record_id < b.record_id
GROUP BY 1, 2
""")

# 3. Scoring (threshold = 4.0)
con.execute("""
CREATE TABLE int_identity__scoring AS
WITH scored AS (
    SELECT
        p.record_a,
        p.record_b,
        CASE WHEN a.email = b.email THEN 5.0
             WHEN a.email IS NULL OR b.email IS NULL THEN 0.0
             WHEN split_part(a.email, '@', 1) = split_part(b.email, '@', 1) THEN 3.0
             ELSE -1.5 END as email_score,
        CASE WHEN a.phone = b.phone AND a.phone IS NOT NULL AND a.phone <> '' THEN 4.0
             WHEN a.phone IS NULL OR b.phone IS NULL OR a.phone = '' OR b.phone = '' THEN 0.0
             ELSE -0.5 END as phone_score,
        jaro_winkler_similarity(a.norm_first_name, b.norm_first_name) * 2.0 as first_name_score,
        jaro_winkler_similarity(a.last_name, b.last_name) * 2.0 as last_name_score,
        jaro_winkler_similarity(a.company_name, b.company_name) * 1.5 as company_score
    FROM int_identity__blocking p
    JOIN int_identity__spine a ON p.record_a = a.record_id
    JOIN int_identity__spine b ON p.record_b = b.record_id
)
SELECT *, (email_score + phone_score + first_name_score + last_name_score + company_score) as total_score
FROM scored
WHERE (email_score + phone_score + first_name_score + last_name_score + company_score) >= 4.0
""")

# 4. Clustering (iterative label propagation - 6 passes)
con.execute("""
CREATE TABLE int_identity__clusters AS
WITH matches AS (
    SELECT record_a AS u, record_b AS v FROM int_identity__scoring
    UNION
    SELECT record_b, record_a FROM int_identity__scoring
),
pass_0 AS (
    SELECT s.record_id AS node,
        LEAST(s.record_id, COALESCE(MIN(m.v), s.record_id)) AS cluster_id
    FROM int_identity__spine s
    LEFT JOIN matches m ON s.record_id = m.u
    GROUP BY s.record_id
),
pass_1 AS (
    SELECT p.node, LEAST(MIN(p.cluster_id), COALESCE(MIN(p2.cluster_id), MIN(p.cluster_id))) AS cluster_id
    FROM pass_0 p LEFT JOIN matches m ON p.node = m.u LEFT JOIN pass_0 p2 ON m.v = p2.node
    GROUP BY p.node
),
pass_2 AS (
    SELECT p.node, LEAST(MIN(p.cluster_id), COALESCE(MIN(p2.cluster_id), MIN(p.cluster_id))) AS cluster_id
    FROM pass_1 p LEFT JOIN matches m ON p.node = m.u LEFT JOIN pass_1 p2 ON m.v = p2.node
    GROUP BY p.node
),
pass_3 AS (
    SELECT p.node, LEAST(MIN(p.cluster_id), COALESCE(MIN(p2.cluster_id), MIN(p.cluster_id))) AS cluster_id
    FROM pass_2 p LEFT JOIN matches m ON p.node = m.u LEFT JOIN pass_2 p2 ON m.v = p2.node
    GROUP BY p.node
),
pass_4 AS (
    SELECT p.node, LEAST(MIN(p.cluster_id), COALESCE(MIN(p2.cluster_id), MIN(p.cluster_id))) AS cluster_id
    FROM pass_3 p LEFT JOIN matches m ON p.node = m.u LEFT JOIN pass_3 p2 ON m.v = p2.node
    GROUP BY p.node
),
pass_5 AS (
    SELECT p.node, LEAST(MIN(p.cluster_id), COALESCE(MIN(p2.cluster_id), MIN(p.cluster_id))) AS cluster_id
    FROM pass_4 p LEFT JOIN matches m ON p.node = m.u LEFT JOIN pass_4 p2 ON m.v = p2.node
    GROUP BY p.node
)
SELECT node AS record_id, cluster_id AS resolved_entity_id FROM pass_5
""")

# Metrics
input_count = con.execute("SELECT count(*) FROM int_identity__spine").fetchone()[0]
resolved_count = con.execute("SELECT count(distinct resolved_entity_id) FROM int_identity__clusters").fetchone()[0]
merge_rate = (input_count - resolved_count) / input_count

print(f"Input Records:    {input_count}")
print(f"Golden Records:   {resolved_count}")
print(f"Merge Rate:       {merge_rate:.1%}")
print(f"Compression:      {input_count / resolved_count:.1f}x")
