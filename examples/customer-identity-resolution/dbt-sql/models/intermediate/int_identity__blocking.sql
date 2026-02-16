with spine as (
    select * from {{ ref('int_identity__spine') }}
),

blocking_keys as (
    -- Key 1: exact normalized email
    select record_id, email as match_key from spine where email is not null
    union all
    -- Key 2: exact phone digits
    select record_id, phone from spine where phone is not null and phone <> ''
    union all
    -- Key 3: normalized first + last name
    select record_id, norm_first_name || '|' || last_name from spine where norm_first_name is not null and last_name is not null
    union all
    -- Key 4: email username + last name (catches same person at different domains)
    select record_id, split_part(email, '@', 1) || '|' || last_name from spine where email is not null and last_name is not null
    union all
    -- Key 5: last name + company (catches same household or role)
    select record_id, last_name || '|' || company_name from spine where last_name is not null and company_name is not null and company_name <> ''
),

pairs as (
    select
        a.record_id as record_a,
        b.record_id as record_b,
        count(*) as shared_keys
    from blocking_keys a
    join blocking_keys b on a.match_key = b.match_key and a.record_id < b.record_id
    group by 1, 2
)

select * from pairs
