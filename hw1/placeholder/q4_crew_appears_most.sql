select
    name,
    appear_num
from
    people,
    (
        select
            person_id,
            count(1) as appear_num
        from
            crew
        group by
            person_id
        order by
            appear_num desc
        limit
            20
    ) as T
where
    T.person_id = people.person_id;