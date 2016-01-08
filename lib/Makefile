TARGET= \
	inih \
	death_handler

.PHONY: all $(TARGET)

all: $(TARGET) isal
	@for dir in $(TARGET); do \
		$(MAKE) -C $$dir; \
	done

isal:
	$(MAKE) -C isa-l-2.14.0 libisal.la

clean:
	@for dir in $(TARGET); do \
		$(MAKE) clean -C $$dir; \
	done
