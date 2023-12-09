select
    name
from
    (
        select
            DISTINCT(person_id) as dp_id
        from
            crew
        where
            title_id in (
                select
                    DISTINCT(title_id)
                from
                    crew
                where
                    person_id = (
                        select
                            person_id
                        from
                            people
                        WHERE
                            born = 1967
                            and name = 'Nicole Kidman'
                    )
            )
            and category in ("actress", "actor")
    ) as T,
    people
where
    people.person_id = T.dp_id
order by
    name asc