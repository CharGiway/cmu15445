select
    titles.primary_title,
    ratings.votes
from
    people,
    crew,
    ratings,
    titles
WHERE
    people.person_id = crew.person_id
    and crew.title_id = titles.title_id
    and titles.title_id = ratings.title_id
    and people.name like '%Cruise%'
    and people.born = 1962
order by ratings.votes desc limit 10