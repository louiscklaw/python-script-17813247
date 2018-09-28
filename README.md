### to find the statistics from worksble log file.
1. list URL with 404 error
1. list average time to serve a page
1. list the frequency of the datbase table accessing / and db action
1. list the path with status code 301/302 take place

### to run
`sudo apt install pipenv`
`pipenv shell`
`pipenv install`
`pipenv run python3 workable_log_parse.py 2014-09-03.log`
