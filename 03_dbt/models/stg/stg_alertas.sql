select
    md5(l._doc:timestamp::string)    as hash_id,
    a.value:problema::string         as problema,
    a.value:recomendacao::string     as recomendacao,
    a.value:risco::string            as risco
from {{ source('AGROSMART', 'LEITURAS_SENSORES') }} l,
     lateral flatten(input => l._doc:alertas) a