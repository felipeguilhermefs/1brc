.PHONY: run
run:
	INPUT_FILE=1brc.csv PARALLELISM=24 luajit 1brc.lua

