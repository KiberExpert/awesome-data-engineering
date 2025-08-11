mkdir elib_dbt\models\staging\stg_books,elib_dbt\models\staging\stg_authors,elib_dbt\models\staging\stg_users,elib_dbt\models\staging\stg_loans,elib_dbt\models\mart\dim,elib_dbt\models\mart\fact,elib_dbt\snapshots,elib_dbt\seeds,elib_dbt\macros,elib_dbt\tests,elib_dbt\analysis,elib_dbt\docs

ni elib_dbt\models\staging\stg_books\stg_books__source.sql,elib_dbt\models\staging\stg_books\stg_books__tests.yml,
elib_dbt\models\staging\stg_authors\stg_authors__source.sql,elib_dbt\models\staging\stg_authors\stg_authors__tests.yml,
elib_dbt\models\staging\stg_users\stg_users__source.sql,elib_dbt\models\staging\stg_users\stg_users__tests.yml,
elib_dbt\models\staging\stg_loans\stg_loans__source.sql,elib_dbt\models\staging\stg_loans\stg_loans__tests.yml | % { ni $_ }

ni elib_dbt\models\mart\dim\dim_books.sql,elib_dbt\models\mart\dim\dim_authors.sql,elib_dbt\models\mart\dim\dim_users.sql,
elib_dbt\models\mart\fact\fact_loans.sql,elib_dbt\models\mart\fact\fact_book_usage.sql | % { ni $_ }

ni elib_dbt\models\schema.yml,
elib_dbt\snapshots\users_snapshots.sql,elib_dbt\snapshots\books_snapshots.sql,
elib_dbt\seeds\seed_genres.csv,elib_dbt\seeds\seed_languages.csv,elib_dbt\seeds\seed_publishers.csv,
elib_dbt\macros\utils.sql,elib_dbt\macros\date_helpers.sql,
elib_dbt\tests\custom_tests.sql,
elib_dbt\analysis\popular_books_analysis.sql,elib_dbt\analysis\overdue_loans_analysis.sql,
elib_dbt\docs\README.md,
elib_dbt\.gitignore,elib_dbt\dbt_project.yml,elib_dbt\packages.yml | % { ni $_ }

Write-Host "Successfull !"
