.PHONY: demo eval-dev eval-staging eval-production

demo:
	@bash scripts/smoke_test.sh

eval-dev:
	@docker compose run --rm evaluation-eventid

eval-staging:
	@docker compose run --rm evaluation-eventid-staging

eval-production:
	@docker compose run --rm evaluation-eventid-production

