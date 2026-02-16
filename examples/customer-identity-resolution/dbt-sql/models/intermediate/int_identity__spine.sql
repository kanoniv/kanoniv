with raw_union as (
    -- CRM
    select
        crm_contact_id as source_id,
        'crm' as source_system,
        first_name,
        last_name,
        email,
        phone,
        company_name
    from {{ ref('crm_contacts') }}

    union all

    -- Billing ("Last, First" in account_name)
    select
        billing_account_id,
        'billing',
        trim(split_part(account_name, ',', 2)) as first_name,
        trim(split_part(account_name, ',', 1)) as last_name,
        email,
        null as phone,
        company_name
    from {{ ref('billing_accounts') }}

    union all

    -- Support (display_name = "First Last")
    select
        support_user_id,
        'support',
        split_part(display_name, ' ', 1) as first_name,
        split_part(display_name, ' ', 2) as last_name,
        email,
        phone,
        company
    from {{ ref('support_users') }}

    union all

    -- App
    select
        app_user_id,
        'app',
        first_name,
        last_name,
        email,
        null as phone,
        null as company_name
    from {{ ref('app_signups') }}

    union all

    -- Partners
    select
        partner_lead_id,
        'partners',
        first_name,
        last_name,
        email,
        null as phone,
        company
    from {{ ref('partner_leads') }}
),

normalized as (
    select
        source_id,
        source_system,
        {{ normalize_name('first_name') }} as first_name,
        {{ normalize_name('last_name') }} as last_name,
        {{ normalize_nickname(normalize_name('first_name')) }} as norm_first_name,
        {{ normalize_email('email') }} as email,
        {{ normalize_phone('phone') }} as phone,
        lower(trim(company_name)) as company_name,
        md5(source_system || source_id) as record_id
    from raw_union
)

select * from normalized
