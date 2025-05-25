select
    us.id as user_id,
    us.username,
    rl.name as role_name
from users us
inner join user_roles sr on us.id = sr.user_id
inner join roles rl on sr.role_id = rl.id
