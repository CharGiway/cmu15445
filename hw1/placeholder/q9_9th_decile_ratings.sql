select
    name,
    round(avg_rating, 2) as avg_rating
from
    (
        select
            name,
            NTILE(10) over(
                order by
                    avg_rating
            ) as decile,
            avg_rating
        from
            (
                select
                    name,
                    avg(rating) as avg_rating
                from
                    ratings,
                    (
                        select
                            DISTINCT(crew.title_id) as title_id,
                            name,
                            people.person_id
                        from
                            crew,
                            people,
                            titles
                        where
                            people.born = 1955
                            and crew.person_id = people.person_id
                            and titles.title_id = crew.title_id
                            and titles.type = 'movie'
                    ) as T
                where
                    T.title_id = ratings.title_id
                group by
                    T.person_id
            )
    )
where
    decile = 9
order by
    avg_rating desc,
    name asc