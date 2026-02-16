with pairs as (
    select * from {{ ref('int_identity__blocking') }}
),

spine as (
    select * from {{ ref('int_identity__spine') }}
),

scored as (
    select
        p.record_a,
        p.record_b,

        case
            when a.email = b.email then 5.0
            when a.email is null or b.email is null then 0.0
            when split_part(a.email, '@', 1) = split_part(b.email, '@', 1) then 3.0
            else -1.5
        end as email_score,

        case
            when a.phone = b.phone and a.phone is not null and a.phone <> '' then 4.0
            when a.phone is null or b.phone is null or a.phone = '' or b.phone = '' then 0.0
            else -0.5
        end as phone_score,

        {{ calculate_similarity('a.norm_first_name', 'b.norm_first_name', 'jaro_winkler') }} * 2.0 as first_name_score,

        {{ calculate_similarity('a.last_name', 'b.last_name', 'jaro_winkler') }} * 2.0 as last_name_score,

        {{ calculate_similarity('a.company_name', 'b.company_name', 'jaro_winkler') }} * 1.5 as company_score

    from pairs p
    join spine a on p.record_a = a.record_id
    join spine b on p.record_b = b.record_id
),

final as (
    select
        *,
        (email_score + phone_score + first_name_score + last_name_score + company_score) as total_score
    from scored
)

select * from final
where total_score >= 4.0
