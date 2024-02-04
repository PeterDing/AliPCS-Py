typecheck:
	ruff check alipcs_py


format-check:
	ruff format --check .

format:
	ruff format .

build-pyx:
	python build.py build_ext --inplace


build: all
	rm -fr dist
	poetry build -f sdist

publish: all
	poetry publish

build-publish: build publish

all: format-check typecheck
