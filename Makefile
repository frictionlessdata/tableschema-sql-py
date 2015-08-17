
test: install
	@rm -rf **/*.pyc
	@pyenv/bin/nosetests --with-coverage --cover-package=jtssql --cover-erase

schema:
	@pyenv/bin/jtskit infer --to_file=tests/fixtures/countries.csv.json tests/fixtures/countries.csv
	@pyenv/bin/jtskit infer --to_file=tests/fixtures/sa.csv.json tests/fixtures/sa.csv

install: pyenv/bin/python

pyenv/bin/python:
	virtualenv-3.4 pyenv
	pyenv/bin/pip install --upgrade pip
	pyenv/bin/pip install wheel nose coverage unicodecsv jtskit typecast
	pyenv/bin/pip install -e .

upload: clean install
	pyenv/bin/python setup.py sdist bdist_wheel upload

clean:
	rm -rf pyenv
