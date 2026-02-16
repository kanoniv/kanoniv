{% macro normalize_email(column) %}
    lower(trim(
        case
            when split_part({{ column }}, '@', 2) in ('gmail.com', 'googlemail.com')
            then replace(split_part(split_part({{ column }}, '@', 1), '+', 1), '.', '') || '@gmail.com'
            when {{ column }} like '%+%'
            then split_part({{ column }}, '+', 1) || '@' || split_part({{ column }}, '@', 2)
            else {{ column }}
        end
    ))
{% endmacro %}

{% macro normalize_phone(column) %}
    regexp_replace({{ column }}, '[^0-9]', '', 'g')
{% endmacro %}

{% macro normalize_name(column) %}
    lower(trim({{ column }}))
{% endmacro %}

{% macro normalize_nickname(column) %}
    case {{ column }}
        when 'bob' then 'robert'
        when 'bobby' then 'robert'
        when 'rob' then 'robert'
        when 'bill' then 'william'
        when 'billy' then 'william'
        when 'will' then 'william'
        when 'mike' then 'michael'
        when 'mikey' then 'michael'
        when 'jim' then 'james'
        when 'jimmy' then 'james'
        when 'jamie' then 'james'
        when 'liz' then 'elizabeth'
        when 'beth' then 'elizabeth'
        when 'lizzy' then 'elizabeth'
        when 'jen' then 'jennifer'
        when 'jenny' then 'jennifer'
        when 'kate' then 'katherine'
        when 'kathy' then 'katherine'
        when 'katie' then 'katherine'
        when 'dick' then 'richard'
        when 'rick' then 'richard'
        when 'rich' then 'richard'
        when 'tom' then 'thomas'
        when 'tommy' then 'thomas'
        when 'dan' then 'daniel'
        when 'danny' then 'daniel'
        when 'dave' then 'david'
        when 'ed' then 'edward'
        when 'ted' then 'edward'
        when 'joe' then 'joseph'
        when 'joey' then 'joseph'
        when 'chris' then 'christopher'
        when 'matt' then 'matthew'
        when 'pat' then 'patrick'
        when 'steve' then 'stephen'
        when 'tony' then 'anthony'
        when 'nick' then 'nicholas'
        when 'alex' then 'alexander'
        when 'sam' then 'samuel'
        when 'ben' then 'benjamin'
        when 'charlie' then 'charles'
        when 'chuck' then 'charles'
        when 'sue' then 'susan'
        when 'suzy' then 'susan'
        when 'meg' then 'margaret'
        when 'maggie' then 'margaret'
        when 'peggy' then 'margaret'
        else {{ column }}
    end
{% endmacro %}

{% macro calculate_similarity(col_a, col_b, method='jaro_winkler') %}
    {% if method == 'jaro_winkler' %}
        jaro_winkler_similarity({{ col_a }}, {{ col_b }})
    {% else %}
        case when {{ col_a }} = {{ col_b }} then 1.0 else 0.0 end
    {% endif %}
{% endmacro %}
