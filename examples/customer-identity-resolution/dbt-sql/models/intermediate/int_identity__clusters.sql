{# Connected components via iterative label propagation.
   Each pass propagates the minimum cluster ID across edges.
   6 passes handles clusters up to diameter 6 (sufficient for identity graphs). #}

{% set num_passes = 6 %}

with matches as (
    select record_a as u, record_b as v from {{ ref('int_identity__scoring') }}
    union
    select record_b, record_a from {{ ref('int_identity__scoring') }}
),

-- Pass 0: each node starts with the minimum of itself and its direct neighbors
pass_0 as (
    select
        s.record_id as node,
        least(s.record_id, coalesce(min(m.v), s.record_id)) as cluster_id
    from {{ ref('int_identity__spine') }} s
    left join matches m on s.record_id = m.u
    group by s.record_id
)

{% for i in range(1, num_passes + 1) %}
, pass_{{ i }} as (
    select
        p.node,
        least(min(p.cluster_id), coalesce(min(p2.cluster_id), min(p.cluster_id))) as cluster_id
    from pass_{{ i - 1 }} p
    left join matches m on p.node = m.u
    left join pass_{{ i - 1 }} p2 on m.v = p2.node
    group by p.node
)
{% endfor %}

select node as record_id, cluster_id as resolved_entity_id from pass_{{ num_passes }}
