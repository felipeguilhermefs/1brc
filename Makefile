.PHONY: small
small:
	INPUT_FILE=small.csv PARALLELISM=2 luajit 1brc.lua

.PHONY: 1brc
1brc:
	INPUT_FILE=1brc.csv PARALLELISM=12 luajit 1brc.lua

