from __future__ import annotations

from rich.console import Console

from config.database import Database
from config.schema_manager import SchemaManager
from config.settings import SettingsError, load_settings

console = Console()


def bootstrap() -> None:
	settings = load_settings()
	database = Database(settings=settings)
	schema_manager = SchemaManager(database=database)
	schema_manager.initialize_uc4_schema()

	console.print(
		"[bold green]Startup complete.[/bold green] "
		"UC-01 to UC-04 tables are ready: "
		"GAMBLERS, BETTING_PREFERENCES, SESSIONS, SESSION_PARAMETERS, PAUSE_RECORDS, "
		"BETTING_STRATEGIES, BETS, GAME_RECORDS, STAKE_TRANSACTIONS, RUNNING_TOTALS_SNAPSHOTS."
	)


def main() -> None:
	try:
		bootstrap()
	except SettingsError as exc:
		console.print(f"[bold red]Invalid configuration:[/bold red] {exc}")
		raise SystemExit(1) from exc
	except Exception as exc:
		console.print(f"[bold red]Application startup failed:[/bold red] {exc}")
		raise SystemExit(1) from exc


if __name__ == "__main__":
	main()