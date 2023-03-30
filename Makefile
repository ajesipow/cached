help: ## Show this help message
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

check: c ## Run all static checks (like linting and pre-commit hooks)

c: c-clippy c-fmt ## Run the clippy and formatter

c-clippy:  ## Run the clippy check
	cargo clippy --all-targets --all-features -- -D warnings

c-fmt: update-nightly-fmt ## Run the fmt check
	cargo +nightly-2023-03-27 fmt --all -- --check

format: update-nightly-fmt ## Format the code
	cargo +nightly-2023-03-27 fmt --all

update-nightly-fmt: ## Installs/updates the nightly rustfmt installation
	rustup toolchain install --profile minimal nightly-2023-03-27 --no-self-update
	rustup component add rustfmt --toolchain nightly-2023-03-27

all: format check ## Formats and checks the code

test: ## Run all tests
	cargo test --lib --bins --tests
	cargo test --doc -- --test-threads 1


# --------------Configuration-------------
.NOTPARALLEL: ; # wait for this target to finish
.EXPORT_ALL_VARIABLES: ; # send all vars to shell
.PHONY: docs all # All targets are accessible for user
.DEFAULT: help # Running Make will run the help target

MAKEFLAGS += --no-print-directory # dont add message about entering and leaving the working directory
