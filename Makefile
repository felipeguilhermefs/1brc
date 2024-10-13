.PHONY: run
run:
	INPUT_FILE=1brc.csv PARALLELISM=12 luajit 1brc.lua

