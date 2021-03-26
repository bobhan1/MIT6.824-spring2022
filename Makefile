# This is the Makefile helping you submit the labs.
# Submit your lab with the following command:
#     $ make [lab1|lab2a|lab2b|lab2c|lab2d|lab3a|lab3b|lab4a|lab4b]

COURSE=cs451
LABS=" lab1 lab2a lab2b lab2c lab2d lab3a lab3b lab4a lab4b "

%:
	@echo "Preparing $@-handin.tar.gz"
	@if echo $(LABS) | grep -q " $@ " ; then \
		echo "Tarring up your submission..." ; \
		tar cvzf $@-handin.tar.gz \
			"--exclude=src/main/pg-*.txt" \
			"--exclude=src/main/diskvd" \
			"--exclude=src/mapreduce/824-mrinput-*.txt" \
			"--exclude=src/main/mr-*" \
			"--exclude=mrtmp.*" \
			"--exclude=src/main/diff.out" \
			"--exclude=src/main/mrmaster" \
			"--exclude=src/main/mrsequential" \
			"--exclude=src/main/mrworker" \
			"--exclude=*.so" \
			"--exclude=src/.gitignore" \
			Makefile src; \
		echo "Are you sure you want to submit $@? Enter 'yes' to continue:"; \
		read line; \
		if test "$$line" != "yes" ; then echo "Giving up submission"; exit; fi; \
		if test `stat -c "%s" "$@-handin.tar.gz" 2>/dev/null || stat -f "%z" "$@-handin.tar.gz"` -ge 20971520 ; then echo "File exceeds 20MB."; exit; fi; \
		gsubmit $(COURSE) $@-handin.tar.gz; \
	else \
		echo "Bad target $@. Usage: make [$(LABS)]"; \
	fi
