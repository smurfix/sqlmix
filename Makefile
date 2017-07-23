#!/usr/bin/make -f

all:
	@echo "Please use 'python setup.py'."
	@exit 1

pypi:
	python3 setup.py sdist upload
	git tag v$(shell python3 setup.py -V)

upload:	pypi
	git push-all --tags
