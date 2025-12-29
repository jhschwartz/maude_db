PYTHON := python3.11

setup:
	$(PYTHON) -m venv venv
	venv/bin/pip install --upgrade pip
	venv/bin/pip install -r requirements.txt

install:
	venv/bin/pip install -r requirements.txt

test:
	venv/bin/python -m unittest discover tests -v

clean:
	rm -rf venv
	rm -rf maude_data/*.zip
	rm -rf __pycache__ tests/__pycache__
	rm -f *.db

.DEFAULT_GOAL := setup