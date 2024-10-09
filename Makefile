.PHONY: small
small:
	luajit 1brc.lua small.csv

.PHONY: 1brc
1brc:
	luajit 1brc.lua 1brc.csv

