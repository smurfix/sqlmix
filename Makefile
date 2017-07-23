#!/usr/bin/make -f

pypi:
	python3 setup.py sdist upload
	git tag v$(shell python3 setup.py -V)

upload:	pypi
	git push-all --tags
