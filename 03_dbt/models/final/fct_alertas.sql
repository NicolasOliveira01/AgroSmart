select
    hash_id,
    problema,
    initcap(replace(problema, '_', ' ')) as problema_descricao,
    recomendacao,
    risco,
    case risco
        when 'Baixo' then 1
        when 'Médio' then 2
        when 'Alto'  then 3
    end as nivel_risco
from {{ ref('stg_alertas') }}