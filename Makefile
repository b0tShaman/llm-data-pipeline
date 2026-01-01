# Makefile for Data Pipeline

BINARY_NAME=data-pipe
PYTHON=python3  # Default value
GO_FLAGS=-ldflags "-s -w -X main.mode=$(MODE) -X main.pythonCmd=$(PYTHON)"
GC_FLAGS=GOGC=200

.PHONY: all clean wiki reddit stack

# Generic runner helper
run:
	@echo "--- Launching $(MODE) Pipeline with $(PYTHON) ---"
	$(GC_FLAGS) go run $(GO_FLAGS) .

# Specific targets
wiki:
	$(MAKE) run MODE=wiki

reddit:
	$(MAKE) run MODE=reddit

stack:
	$(MAKE) run MODE=stack

clean:
	rm -f $(BINARY_NAME) dataset_*.txt